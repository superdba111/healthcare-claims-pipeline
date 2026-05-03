"""
GOLD LAYER: Create Iceberg tables from silver data
Using AWS Glue Data Catalog + Iceberg format
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
    """Gold layer: Create Iceberg tables with Glue Catalog"""
    
    try:
        logger.info("="*60)
        logger.info("GOLD LAYER - Creating Iceberg Tables")
        logger.info("="*60)
        
        # Get silver file
        silver_file = event.get('silver_file')
        if not silver_file:
            silver_file = get_latest_silver_file()
        
        if not silver_file:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No silver file'})}
        
        logger.info(f"Processing: {silver_file}")
        
        # Read silver data
        response = s3.get_object(Bucket=BUCKET, Key=silver_file)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        logger.info(f"Read {len(df)} rows")
        
        # Create Glue Database (if not exists)
        create_glue_database()
        
        # 🚨 ONE-TIME NUCLEAR RESET: Clean up any ghost tables or S3 metadata markers 🚨
        # force_cleanup_corrupted_state()
        
        # Write data to staging
        staging_location = write_staging_data(df)
        
        # Create Iceberg tables using proper syntax
        create_iceberg_tables()
        
        # Load data into Iceberg tables
        load_data_to_iceberg(staging_location)
        
        # Update Glue Catalog SAFELY
        update_glue_catalog()
        
        logger.info(f"✅ GOLD LAYER COMPLETE")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Gold layer complete',
                'database': DATABASE,
                'tables': ['fact_claims', 'dim_provider', 'dim_procedure'],
                'catalog': 'AWS Glue Data Catalog',
                'staging_location': staging_location
            })
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}


def force_cleanup_corrupted_state():
    """Force delete Glue tables and wipe S3 Iceberg paths to bypass Athena ghost state"""
    tables_to_nuke = ['fact_claims', 'dim_provider', 'dim_procedure']
    logger.info("🧹 Initiating forced cleanup of previous corrupted state...")
    
    # 1. Force delete the tables from the Glue Data Catalog
    for table_name in tables_to_nuke:
        try:
            glue.delete_table(DatabaseName=DATABASE, Name=table_name)
            logger.info(f"🧹 Force deleted '{table_name}' from Glue Catalog.")
        except Exception:
            pass # Ignore if it doesn't exist

    # 2. Force delete all files (including hidden delete markers) in S3
    for table_name in tables_to_nuke:
        prefix = f"gold/iceberg/{table_name}/"
        try:
            # Paginate in case there are many metadata files
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    objects = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3.delete_objects(Bucket=BUCKET, Delete={'Objects': objects})
            logger.info(f"🧹 Cleared out raw S3 path: {prefix}")
        except Exception as e:
            logger.warning(f"Failed to clear S3 path {prefix}: {e}")


def create_glue_database():
    """Create Glue Catalog database"""
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
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=BUCKET, Key=staging_key, Body=buffer.getvalue())
    
    logger.info(f"✅ Staging data written: {staging_key} ({len(df)} rows)")
    return f"s3://{BUCKET}/{staging_key}"


def create_iceberg_tables():
    """Create Iceberg tables using proper Athena syntax"""
    
    # 1. Drop existing staging table to ensure schema updates
    drop_staging_sql = f"DROP TABLE IF EXISTS {DATABASE}.staging_claims"
    execute_athena_query(drop_staging_sql, wait=True)
    logger.info("✅ Cleared old staging table schema")

    # 2. Create staging table (matching Pandas DOUBLE outputs)
    staging_sql = f"""
    CREATE EXTERNAL TABLE {DATABASE}.staging_claims (
        claim_item_id BIGINT,
        claimant_id BIGINT,
        claim_code STRING,
        provider_npi STRING,
        service_provider STRING,
        provider_city STRING,
        provider_state STRING,
        charge_amt DOUBLE,
        allowed_amt DOUBLE,
        discount_amt DOUBLE,
        discount_pct DOUBLE,
        units INT,
        received_date DATE,
        year INT,
        month INT,
        oi_in_network STRING,
        rev_code_procedure_description STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/gold/staging/'
    """
    execute_athena_query(staging_sql, wait=True)
    logger.info("✅ Staging table created")
    
    # 3. Create Iceberg fact table (Keep as DECIMAL)
    fact_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.fact_claims (
        claim_item_id BIGINT,
        claimant_id BIGINT,
        claim_code STRING,
        provider_npi STRING,
        charge_amt DECIMAL(10,2),
        allowed_amt DECIMAL(10,2),
        discount_amt DECIMAL(10,2),
        units INT,
        received_date DATE,
        year INT,
        month INT,
        in_network BOOLEAN,
        _loaded_at TIMESTAMP
    )
    PARTITIONED BY (year, month)
    LOCATION 's3://{BUCKET}/gold/iceberg/fact_claims/'
    TBLPROPERTIES ('table_type'='ICEBERG')
    """
    execute_athena_query(fact_sql, wait=True)
    logger.info("✅ Iceberg fact table created")
    
    # 4. Create dimension tables
    dim_provider_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.dim_provider (
        provider_npi STRING,
        provider_name STRING,
        city STRING,
        state STRING,
        _loaded_at TIMESTAMP
    )
    LOCATION 's3://{BUCKET}/gold/iceberg/dim_provider/'
    TBLPROPERTIES ('table_type'='ICEBERG')
    """
    execute_athena_query(dim_provider_sql, wait=True)
    logger.info("✅ Iceberg dim_provider table created")
    
    dim_procedure_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.dim_procedure (
        claim_code STRING,
        procedure_desc STRING,
        _loaded_at TIMESTAMP
    )
    LOCATION 's3://{BUCKET}/gold/iceberg/dim_procedure/'
    TBLPROPERTIES ('table_type'='ICEBERG')
    """
    execute_athena_query(dim_procedure_sql, wait=True)
    logger.info("✅ Iceberg dim_procedure table created")


def load_data_to_iceberg(staging_location):
    """Load data from staging to Iceberg tables"""
    
    # Load fact table with explicit casts for decimals
    load_fact_sql = f"""
    INSERT INTO {DATABASE}.fact_claims
    SELECT 
        claim_item_id,
        claimant_id,
        claim_code,
        provider_npi,
        CAST(charge_amt AS DECIMAL(10,2)),
        CAST(allowed_amt AS DECIMAL(10,2)),
        CAST(discount_amt AS DECIMAL(10,2)),
        units,
        received_date,
        year,
        month,
        CASE WHEN oi_in_network = 'Y' THEN true ELSE false END as in_network,
        CURRENT_TIMESTAMP as _loaded_at
    FROM {DATABASE}.staging_claims
    """
    execute_athena_query(load_fact_sql, wait=True)
    logger.info("✅ Data loaded to fact_claims")
    
    # Load dim_provider
    load_provider_sql = f"""
    INSERT INTO {DATABASE}.dim_provider
    SELECT DISTINCT
        provider_npi,
        service_provider as provider_name,
        provider_city as city,
        provider_state as state,
        CURRENT_TIMESTAMP as _loaded_at
    FROM {DATABASE}.staging_claims
    WHERE provider_npi IS NOT NULL
    """
    execute_athena_query(load_provider_sql, wait=True)
    logger.info("✅ Data loaded to dim_provider")
    
    # Load dim_procedure
    load_procedure_sql = f"""
    INSERT INTO {DATABASE}.dim_procedure
    SELECT DISTINCT
        claim_code,
        rev_code_procedure_description as procedure_desc,
        CURRENT_TIMESTAMP as _loaded_at
    FROM {DATABASE}.staging_claims
    WHERE claim_code IS NOT NULL
    """
    execute_athena_query(load_procedure_sql, wait=True)
    logger.info("✅ Data loaded to dim_procedure")


def execute_athena_query(sql, wait=False, max_wait=300):
    """Execute Athena query and optionally wait for completion using polling"""
    try:
        logger.debug(f"Executing SQL: {sql[:200]}...")
        
        response = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        
        execution_id = response['QueryExecutionId']
        logger.info(f"Query started: {execution_id}")
        
        if wait:
            start_time = time.time()
            while True:
                status_response = athena.get_query_execution(QueryExecutionId=execution_id)
                state = status_response['QueryExecution']['Status']['State']
                
                if state == 'SUCCEEDED':
                    logger.info(f"✅ Query {execution_id} succeeded.")
                    break
                elif state in ['FAILED', 'CANCELLED']:
                    reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
                    logger.error(f"❌ Query {execution_id} {state}: {reason}")
                    raise Exception(f"Athena query {state}: {reason}")
                
                if time.time() - start_time > max_wait:
                    raise TimeoutError(f"Query {execution_id} timed out after {max_wait} seconds")
                    
                time.sleep(2)
            
        return execution_id
        
    except Exception as e:
        logger.error(f"Athena query execution failed: {e}")
        raise


def update_glue_catalog():
    """Update Glue Catalog with table metadata SAFELY"""
    
    try:
        # 1. Fetch the existing table definition created by Athena
        existing_table = glue.get_table(DatabaseName=DATABASE, Name='fact_claims')['Table']
        
        # 2. Remove system-generated keys that will cause update_table to fail
        keys_to_remove = [
            'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 
            'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'FederatedTable'
        ]
        for key in keys_to_remove:
            existing_table.pop(key, None)
            
        # 3. Update description and append our custom parameters
        existing_table['Description'] = 'Fact table for healthcare claim line items - One row per claim line'
        
        if 'Parameters' not in existing_table:
            existing_table['Parameters'] = {}
            
        existing_table['Parameters'].update({
            'source_layer': 'gold',
            'update_frequency': 'daily',
            'data_quality': 'validated'
        })
        
        # 4. Safely push the full definition back to Glue
        glue.update_table(
            DatabaseName=DATABASE,
            TableInput=existing_table
        )
        logger.info("✅ Glue Catalog metadata updated safely")
        
    except Exception as e:
        logger.warning(f"Could not update table metadata: {e}")


def get_latest_silver_file():
    """Find latest parquet in silver folder"""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='silver/')
    
    if 'Contents' not in response:
        logger.info("No files in silver folder")
        return None
    
    parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
    
    if not parquet_files:
        logger.info("No parquet files in silver folder")
        return None
    
    latest = max(parquet_files, key=lambda x: x['LastModified'])
    logger.info(f"Latest silver file: {latest['Key']}")
    return latest['Key']