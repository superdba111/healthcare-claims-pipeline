# silver_clean_fixed.py
"""
SILVER LAYER: Clean and validate data from bronze
Fixed NPI extraction from service_address_3
Reads: s3://hc-pipeline-demo/bronze/*.parquet
Writes: s3://hc-pipeline-demo/silver/cleaned_*.parquet
"""

import boto3
import pandas as pd
import numpy as np
import re
from io import BytesIO
from datetime import datetime
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
BUCKET = 'hc-pipeline-demo'


def lambda_handler(event, context):
    """Silver layer: Clean data from bronze"""
    
    try:
        logger.info("="*60)
        logger.info("SILVER LAYER STARTING")
        logger.info("="*60)
        
        # 1. Find the latest bronze file
        bronze_file = get_latest_bronze_file()
        
        if not bronze_file:
            logger.info("No bronze files found. Exiting.")
            return {'statusCode': 200, 'body': 'No bronze files to process'}
        
        logger.info(f"Processing bronze file: {bronze_file}")
        
        # 2. Read bronze Parquet
        df = read_bronze_file(bronze_file)
        logger.info(f"Read {len(df)} rows from bronze")
        
        # 3. Clean the data (Silver transformations with fixed NPI extraction)
        df_clean = clean_data(df)
        logger.info(f"Cleaned {len(df_clean)} rows")
        
        # 4. Add validation metadata
        df_clean = add_validation_metadata(df_clean)
        
        # 5. Save to silver folder
        output_file = save_to_silver(df_clean)
        
        # 6. Create data quality report
        quality_report = generate_quality_report(df_clean)
        
        logger.info(f"✅ SILVER LAYER COMPLETE")
        logger.info(f"   Output: {output_file}")
        logger.info(f"   Quality score: {quality_report['quality_score']:.2f}%")
        logger.info(f"   NPIs extracted: {quality_report.get('npi_count', 0)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Silver layer complete',
                'input_file': bronze_file,
                'output_file': output_file,
                'rows_processed': len(df_clean),
                'quality_report': quality_report
            })
        }
        
    except Exception as e:
        logger.error(f"Silver layer error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def get_latest_bronze_file():
    """Find the most recent parquet file in bronze folder"""
    
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix='bronze/')
        
        if 'Contents' not in response:
            logger.info("No files in bronze folder")
            return None
        
        parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        
        if not parquet_files:
            logger.info("No parquet files in bronze folder")
            return None
        
        latest = max(parquet_files, key=lambda x: x['LastModified'])
        logger.info(f"Latest bronze file: {latest['Key']}")
        return latest['Key']
        
    except Exception as e:
        logger.error(f"Error listing bronze files: {e}")
        return None


def read_bronze_file(file_key):
    """Read parquet file from S3"""
    
    response = s3.get_object(Bucket=BUCKET, Key=file_key)
    return pd.read_parquet(BytesIO(response['Body'].read()))


def extract_npi(address):
    """
    Extract 10-digit NPI from address string
    Handles multiple formats:
    - '8885 VENICE BLVD STE 104 - 900343242' (with 9-digit zip)
    - 'PO BOX 95970 - 840950970' 
    - '6 BENDIX - 926182006'
    - Any 10-digit number in the string
    """
    if pd.isna(address) or address == '':
        return None
    
    address_str = str(address)
    
    # Method 1: Look for exactly 10 consecutive digits (standard NPI format)
    match = re.search(r'\b(\d{10})\b', address_str)
    if match:
        return match.group(1)
    
    # Method 2: Look for digits after ' - ' separator
    if ' - ' in address_str:
        parts = address_str.split(' - ')
        if len(parts) > 1:
            # Get the part after the dash
            after_dash = parts[1].strip()
            # Extract any digits from that part
            digits = re.findall(r'\d+', after_dash)
            if digits:
                # Return the longest digit sequence (likely the NPI)
                longest = max(digits, key=len)
                if len(longest) >= 9:  # NPI can be 9 or 10 digits
                    return longest
    
    # Method 3: Find any 9-10 digit sequence in the entire string
    match = re.search(r'\b(\d{9,10})\b', address_str)
    if match:
        return match.group(1)
    
    return None


def parse_city_state(address):
    """Parse city and state from service_address_2"""
    if pd.isna(address) or address == '':
        return None, None
    
    address_str = str(address)
    
    # Split by comma
    if ',' in address_str:
        parts = address_str.split(',')
        city = parts[0].strip()
        # Extract state (2 letters) from the second part
        state_match = re.search(r'([A-Z]{2})', parts[1].strip() if len(parts) > 1 else '')
        state = state_match.group(1) if state_match else None
        return city, state
    
    return address_str, None


def clean_data(df):
    """SILVER: Clean and validate data with fixed NPI extraction"""
    
    # Make column names consistent (lowercase, no spaces)
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # ===== FIX DATA TYPES =====
    
    # Date column
    if 'received_date' in df.columns:
        df['received_date'] = pd.to_datetime(df['received_date']).dt.date
    
    # Numeric columns
    numeric_cols = ['claimant_id', 'claim_item_id', 'charge_amt', 'allowed_amt', 'units']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Fill nulls with defaults
    df['charge_amt'] = df['charge_amt'].fillna(0)
    df['allowed_amt'] = df['allowed_amt'].fillna(0)
    df['units'] = df['units'].fillna(1).astype(int)
    
    # ===== CLEAN STRING COLUMNS =====
    
    # Clean network flag
    if 'oi_in_network' in df.columns:
        df['oi_in_network'] = df['oi_in_network'].fillna('N').astype(str).str.upper()
        df['oi_in_network'] = df['oi_in_network'].apply(lambda x: 'Y' if x == 'Y' else 'N')
    
    # Clean claim type (impute missing)
    if 'type' in df.columns:
        df['claim_type'] = df['type'].fillna('').astype(str).str.upper()
        df['claim_type'] = df['claim_type'].apply(lambda x: 'UNKNOWN' if x == '' else x)
    
    # Clean modifiers
    if 'claim_code_modifier' in df.columns:
        df['modifier_1'] = df['claim_code_modifier'].fillna('').astype(str).str.upper()
    if 'claim_code_modifier_2' in df.columns:
        df['modifier_2'] = df['claim_code_modifier_2'].fillna('').astype(str).str.upper()
    
    # ===== PARSE PROVIDER ADDRESS (FIXED NPI EXTRACTION) =====
    
    if 'service_address_3' in df.columns:
        # Extract NPI using improved function
        df['provider_npi'] = df['service_address_3'].apply(extract_npi)
        logger.info(f"Extracted {df['provider_npi'].notna().sum()} NPIs from service_address_3")
    
    if 'service_address_2' in df.columns:
        # Parse city and state
        city_state = df['service_address_2'].apply(parse_city_state)
        df['provider_city'] = [x[0] for x in city_state]
        df['provider_state'] = [x[1] for x in city_state]
    
    # ===== CREATE DERIVED COLUMNS =====
    
    df['discount_amt'] = df['charge_amt'] - df['allowed_amt']
    df['discount_pct'] = np.where(
        df['charge_amt'] > 0,
        (df['discount_amt'] / df['charge_amt'] * 100).round(2),
        0
    )
    
    # ===== ADD PARTITION COLUMNS =====
    
    if 'received_date' in df.columns:
        df['year'] = pd.to_datetime(df['received_date']).dt.year
        df['month'] = pd.to_datetime(df['received_date']).dt.month
        df['quarter'] = pd.to_datetime(df['received_date']).dt.quarter
    
    # ===== DATA QUALITY FLAGS =====
    
    df['has_negative_charge'] = df['charge_amt'] < 0
    df['has_negative_allowed'] = df['allowed_amt'] < 0
    df['has_missing_provider'] = df['service_provider'].isna()
    df['has_npi'] = df['provider_npi'].notna()
    df['is_valid'] = (df['charge_amt'] >= 0) & (df['allowed_amt'] >= 0) & (df['units'] > 0)
    
    return df


def add_validation_metadata(df):
    """Add metadata about the cleaning process"""
    
    df['_cleaned_at'] = datetime.now().isoformat()
    df['_silver_batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
    df['_validation_status'] = df['is_valid'].apply(lambda x: 'PASS' if x else 'FAIL')
    
    return df


def save_to_silver(df):
    """Save cleaned data to silver folder as Parquet"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_key = f"silver/claims_silver_{timestamp}.parquet"
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    s3.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=parquet_buffer.getvalue()
    )
    
    logger.info(f"Saved silver data to: {output_key}")
    return output_key


def generate_quality_report(df):
    """Generate data quality metrics"""
    
    total_rows = len(df)
    
    report = {
        'total_rows': total_rows,
        'quality_score': round((df['is_valid'].sum() / total_rows * 100), 2),
        'null_counts': {},
        'negative_charges': int(df['has_negative_charge'].sum()),
        'negative_allowed': int(df['has_negative_allowed'].sum()),
        'missing_providers': int(df['has_missing_provider'].sum()),
        'npi_count': int(df['has_npi'].sum()),
        'unique_claimants': int(df['claimant_id'].nunique()) if 'claimant_id' in df else 0,
        'unique_codes': int(df['claim_code'].nunique()) if 'claim_code' in df else 0,
        'unique_npis': int(df['provider_npi'].nunique()) if 'provider_npi' in df else 0,
        'date_range': {
            'min': df['received_date'].min().isoformat() if 'received_date' in df else None,
            'max': df['received_date'].max().isoformat() if 'received_date' in df else None
        }
    }
    
    # Count nulls in key columns
    key_cols = ['claimant_id', 'claim_item_id', 'charge_amt', 'claim_code']
    for col in key_cols:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                report['null_counts'][col] = null_count
    
    # Add NPI extraction sample
    if 'provider_npi' in df:
        sample_npis = df[df['provider_npi'].notna()]['provider_npi'].head(5).tolist()
        if sample_npis:
            report['sample_npis'] = sample_npis
    
    return report