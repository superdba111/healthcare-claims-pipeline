import boto3

def apply_cost_optimization_policy(bucket_name):
    """
    Applies S3 lifecycle rules to manage storage costs.
    Moves data to IA after 30 days and Glacier after 90 days.
    """
    s3 = boto3.client('s3')

    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': 'bronze-silver-tiering',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''}, # Applies to all folders
                    'Transitions': [
                        {'Days': 30, 'StorageClass': 'STANDARD_IA'}, # 40% cost reduction
                        {'Days': 90, 'StorageClass': 'GLACIER'},      # 80% cost reduction
                    ],
                    'Expiration': {'Days': 365} # Delete data older than 1 year
                }
            ]
        }
    )
    print(f"Lifecycle policy applied to {bucket_name} for cost optimization.")

if __name__ == "__main__":
    apply_cost_optimization_policy(bucket_name="hc-pipeline-demo")