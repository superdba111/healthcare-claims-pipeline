# lambda_bronze.py - CORRECT VERSION (Only conversion, no cleaning)
"""
BRONZE LAYER ONLY:
- Read raw Excel
- Convert to Parquet format
- Save to bronze/ folder
- NO data cleaning, NO transformations
"""

import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """BRONZE: Only convert Excel to Parquet, nothing else"""
    
    try:
        # Get file info
        if 'Records' in event:
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = event['Records'][0]['s3']['object']['key']
        else:
            bucket = 'hc-pipeline-demo'
            key = 'raw/DETask.xlsx'
        
        logger.info(f"BRONZE: Processing {key}")
        
        # Read Excel AS-IS (no changes)
        response = s3.get_object(Bucket=bucket, Key=key)
        
        # FIX: Added `dtype=str` to treat all incoming data as raw text. 
        # This prevents Parquet from crashing on mixed-type columns, preserving the raw data state.
        df = pd.read_excel(BytesIO(response['Body'].read()), sheet_name='test_data', dtype=str)
        
        # BRONZE ONLY: Convert to Parquet (no cleaning!)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        # Save to bronze folder with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_key = f"bronze/claims_bronze_{timestamp}.parquet"
        
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=parquet_buffer.getvalue()
        )
        
        logger.info(f"✅ BRONZE complete: {len(df)} rows saved to {output_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Bronze layer complete',
                'rows': len(df),
                'output': output_key
            })
        }
        
    except Exception as e:
        logger.error(f"BRONZE error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}