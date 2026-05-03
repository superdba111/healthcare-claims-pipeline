import pandas as pd
import boto3
from io import BytesIO

def generate_and_upload_dim_date(bucket_name, start_year=2020, end_year=2030):
    """
    Generates a healthcare-optimized Time Dimension and uploads to S3 Gold layer.
    Satisfies Requirement 1 and 6 for Fact/Dim modeling.
    """
    # Create date range
    dates = pd.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31")
    df = pd.DataFrame({"full_date": dates})
    
    # Extract standard attributes
    df["date_key"] = df["full_date"].dt.strftime('%Y%m%d').astype(int)
    df["day"] = df["full_date"].dt.day
    df["month"] = df["full_date"].dt.month
    df["month_name"] = df["full_date"].dt.month_name()
    df["quarter"] = df["full_date"].dt.quarter
    df["year"] = df["full_date"].dt.year
    df["is_weekend"] = df["full_date"].dt.dayofweek > 4
    
    # Healthcare Fiscal Year (Example: Starts Oct 1st for Federal programs)
    df["fiscal_year"] = df["full_date"].apply(lambda x: x.year if x.month < 10 else x.year + 1)
    df["fiscal_period"] = "FY" + df["fiscal_year"].astype(str) + "-Q" + df["quarter"].astype(str)

    # Save to Parquet (Cost-effective for Athena)
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    
    # Upload to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name, 
        Key='gold/dim_date/dim_date.parquet', 
        Body=out_buffer.getvalue()
    )
    print(f"Successfully uploaded dim_date to s3://{bucket_name}/gold/dim_date/")

if __name__ == "__main__":
    generate_and_upload_dim_date(bucket_name="hc-pipeline-demo")