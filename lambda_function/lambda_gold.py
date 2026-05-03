"""
GOLD LAYER: Create 7 Apache Iceberg tables from silver data
Architecture: Star Schema (Medallion - Gold) with ACID compliance
Fix: Partitioning by service_month to prevent ICEBERG_TOO_MANY_OPEN_PARTITIONS
"""
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import json
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
glue = boto3.client('glue')
athena = boto3.client('athena')

BUCKET = 'hc-pipeline-demo'
DATABASE = 'healthcare_gold'
ATHENA_OUTPUT = f's3://{BUCKET}/athena-results/'

def lambda_handler(event, context):
    try:
        logger.info("="*60)
        logger.info("GOLD LAYER - Creating Iceberg Tables (Star Schema)")
        logger.info("="*60)
        
        # 1. Get silver file
        silver_file = get_latest_silver_file()
        if not silver_file:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No silver file'})}
        
        # 2. Read silver data
        response = s3.get_object(Bucket=BUCKET, Key=silver_file)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"Read {len(df)} rows from {silver_file}")
        
        # 3. Create Glue Database
        create_glue_database()
        
        # 4. Force cleanup of any locked/corrupted Iceberg states
        force_cleanup_corrupted_state()
        
        # 5. Write data to staging (Bypasses HIVE_BAD_DATA errors)
        staging_location = write_staging_data(df)
        
        # 6. Build all 7 Iceberg Tables
        results = build_iceberg_star_schema()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Gold Iceberg layer complete',
                'database': DATABASE,
                'executions': results,
                'staging_location': staging_location
            })
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def force_cleanup_corrupted_state():
    """Force delete Glue tables and wipe S3 paths to bypass Athena ghost state"""
    tables_to_nuke = [
        "dim_member", "dim_provider", "dim_payer", 
        "dim_procedure", "fact_remittance", "fact_claims"
    ]
    logger.info("🧹 Initiating forced cleanup of corrupted Iceberg state...")
    
    # 1. Force delete the tables directly from the Glue Data Catalog
    for table_name in tables_to_nuke:
        try:
            glue.delete_table(DatabaseName=DATABASE, Name=table_name)
            logger.info(f"🧹 Force deleted '{table_name}' from Glue Catalog.")
        except Exception:
            pass # Ignore if it doesn't exist in Glue

    # 2. Force delete all underlying files in S3
    for table_name in tables_to_nuke:
        prefix = f"gold/iceberg/{table_name}/"
        try:
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3.delete_objects(Bucket=BUCKET, Delete={'Objects': objects})
            logger.info(f"🧹 Cleared out raw S3 path: {prefix}")
        except Exception as e:
            logger.warning(f"Failed to clear S3 path {prefix}: {e}")


def create_glue_database():
    try:
        glue.create_database(
            DatabaseInput={
                'Name': DATABASE,
                'Description': 'Healthcare claims Gold layer - Iceberg tables'
            }
        )
        logger.info(f"✅ Created Glue database: {DATABASE}")
    except glue.exceptions.AlreadyExistsException:
        logger.info(f"Database {DATABASE} already exists")


def write_staging_data(df):
    """Write cleaned data to staging location as Parquet"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    staging_key = f"gold/staging/data_{timestamp}.parquet"
    
    # Force numeric types to float to prevent Athena CAST errors later
    if 'charge_amt' in df.columns: df['charge_amt'] = df['charge_amt'].astype(float)
    if 'allowed_amt' in df.columns: df['allowed_amt'] = df['allowed_amt'].astype(float)
    # Force received_date to string to prevent INT32 HIVE_BAD_DATA errors
    if 'received_date' in df.columns: df['received_date'] = df['received_date'].astype(str)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=staging_key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{staging_key}"


def execute_athena_query(sql, wait=False, max_wait=300):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    execution_id = response['QueryExecutionId']
    
    if wait:
        start_time = time.time()
        while True:
            status_response = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status_response['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                return execution_id
            elif state in ['FAILED', 'CANCELLED']:
                reason = status_response['QueryExecution']['Status'].get('StateChangeReason', '')
                raise Exception(f"Athena query {state}: {reason}")
            if time.time() - start_time > max_wait:
                raise TimeoutError("Query timed out")
            time.sleep(2)
    return execution_id


def build_iceberg_star_schema():
    results = {}
    
    # Step 1: Create Staging Table (The Bridge)
    execute_athena_query(f"DROP TABLE IF EXISTS {DATABASE}.staging_claims", wait=True)
    staging_sql = f"""
    CREATE EXTERNAL TABLE {DATABASE}.staging_claims (
        claim_item_id BIGINT,
        claimant_id BIGINT,
        claim_code STRING,
        provider_npi STRING,
        service_provider STRING,
        city STRING,
        state STRING,
        type STRING,
        charge_amt DOUBLE,
        allowed_amt DOUBLE,
        units INT,
        received_date STRING,
        oi_in_network STRING,
        proc_desc STRING,
        claim_code_modifier STRING,
        claim_code_modifier_2 STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/gold/staging/'
    """
    execute_athena_query(staging_sql, wait=True)
    logger.info("✅ Staging table recreated")

    # Step 2: Define Iceberg Tables (Schema Only)
    iceberg_ddl = {
        "dim_member": f"""
            CREATE TABLE {DATABASE}.dim_member (
                member_id BIGINT, _loaded_at TIMESTAMP
            ) LOCATION 's3://{BUCKET}/gold/iceberg/dim_member/' TBLPROPERTIES ('table_type'='ICEBERG')
        """,
        "dim_provider": f"""
            CREATE TABLE {DATABASE}.dim_provider (
                provider_npi STRING, provider_name STRING, city STRING, state STRING, _loaded_at TIMESTAMP
            ) LOCATION 's3://{BUCKET}/gold/iceberg/dim_provider/' TBLPROPERTIES ('table_type'='ICEBERG')
        """,
        "dim_payer": f"""
            CREATE TABLE {DATABASE}.dim_payer (
                payer_name STRING, _loaded_at TIMESTAMP
            ) LOCATION 's3://{BUCKET}/gold/iceberg/dim_payer/' TBLPROPERTIES ('table_type'='ICEBERG')
        """,
        "dim_procedure": f"""
            CREATE TABLE {DATABASE}.dim_procedure (
                procedure_code STRING, procedure_desc STRING, _loaded_at TIMESTAMP
            ) LOCATION 's3://{BUCKET}/gold/iceberg/dim_procedure/' TBLPROPERTIES ('table_type'='ICEBERG')
        """,
        "fact_remittance": f"""
            CREATE TABLE {DATABASE}.fact_remittance (
                claim_id BIGINT, payment_amount DECIMAL(18,2), carc_code STRING, rarc_code STRING, _loaded_at TIMESTAMP
            ) LOCATION 's3://{BUCKET}/gold/iceberg/fact_remittance/' TBLPROPERTIES ('table_type'='ICEBERG')
        """,
        "fact_claims": f"""
            CREATE TABLE {DATABASE}.fact_claims (
                claim_id BIGINT, 
                member_id BIGINT, 
                provider_npi STRING, 
                payer_name STRING, 
                procedure_code STRING, 
                service_date_key INT, 
                total_charges DECIMAL(18,2), 
                allowed_amt DECIMAL(18,2), 
                units_count INT, 
                in_network BOOLEAN, 
                service_month STRING,
                _loaded_at TIMESTAMP
            ) 
            PARTITIONED BY (service_month)
            LOCATION 's3://{BUCKET}/gold/iceberg/fact_claims/' TBLPROPERTIES ('table_type'='ICEBERG')
        """
    }

    # Execute DDL
    for table, ddl in iceberg_ddl.items():
        execute_athena_query(ddl, wait=True)
        logger.info(f"✅ Defined Iceberg schema for {table}")

    # Step 3: Insert Data into Iceberg Tables
    insert_queries = {
        "dim_member": f"INSERT INTO {DATABASE}.dim_member SELECT DISTINCT claimant_id, CURRENT_TIMESTAMP FROM {DATABASE}.staging_claims WHERE claimant_id IS NOT NULL",
        "dim_provider": f"INSERT INTO {DATABASE}.dim_provider SELECT DISTINCT provider_npi, service_provider, city, state, CURRENT_TIMESTAMP FROM {DATABASE}.staging_claims WHERE provider_npi IS NOT NULL",
        "dim_payer": f"INSERT INTO {DATABASE}.dim_payer SELECT DISTINCT type, CURRENT_TIMESTAMP FROM {DATABASE}.staging_claims WHERE type IS NOT NULL",
        "dim_procedure": f"INSERT INTO {DATABASE}.dim_procedure SELECT DISTINCT claim_code, proc_desc, CURRENT_TIMESTAMP FROM {DATABASE}.staging_claims WHERE claim_code IS NOT NULL",
        "fact_remittance": f"INSERT INTO {DATABASE}.fact_remittance SELECT claim_item_id, CAST(allowed_amt AS DECIMAL(18,2)), claim_code_modifier, claim_code_modifier_2, CURRENT_TIMESTAMP FROM {DATABASE}.staging_claims WHERE claim_code_modifier IS NOT NULL",
        "fact_claims": f"""
            INSERT INTO {DATABASE}.fact_claims
            SELECT 
                claim_item_id, 
                claimant_id, 
                provider_npi, 
                type, 
                claim_code,
                CAST(REPLACE(SUBSTR(received_date, 1, 10), '-', '') AS INT) as service_date_key,
                CAST(charge_amt AS DECIMAL(18,2)), 
                CAST(allowed_amt AS DECIMAL(18,2)), 
                units,
                CASE WHEN UPPER(oi_in_network) = 'Y' THEN true ELSE false END, 
                SUBSTR(received_date, 1, 7) as service_month,
                CURRENT_TIMESTAMP
            FROM {DATABASE}.staging_claims
        """
    }

    for table, query in insert_queries.items():
        try:
            execute_athena_query(query, wait=True)
            results[table] = "SUCCESS"
            logger.info(f"✅ Loaded data into {table}")
        except Exception as e:
            results[table] = f"ERROR: {e}"
            logger.error(f"❌ Failed to load {table}: {e}")

    # Step 4: Handle dim_date separately
    try:
        execute_athena_query(f"DROP TABLE IF EXISTS {DATABASE}.dim_date", wait=True)
        execute_athena_query(f"""
            CREATE TABLE {DATABASE}.dim_date
            WITH (format = 'PARQUET', external_location = 's3://{BUCKET}/gold/iceberg/dim_date/') AS
            WITH date_series AS (
                SELECT CAST(date_column AS DATE) as full_date FROM UNNEST(SEQUENCE(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1' DAY)) AS t(date_column)
            )
            SELECT CAST(format_datetime(full_date, 'yyyyMMdd') AS INT) AS date_key, full_date, EXTRACT(DAY FROM full_date) AS day, EXTRACT(MONTH FROM full_date) AS month, EXTRACT(YEAR FROM full_date) AS year
            FROM date_series
        """, wait=True)
        results["dim_date"] = "SUCCESS"
        logger.info("✅ Created dim_date")
    except Exception as e:
        results["dim_date"] = f"ERROR: {e}"

    return results


def get_latest_silver_file():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='silver/')
    if 'Contents' not in response: return None
    parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    if not parquet_files: return None
    return max(parquet_files, key=lambda x: x['LastModified'])['Key']