"""Create the object-storage bucket that holds all Iceberg data and metadata files."""
import os

import boto3

s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:8333"),
                  aws_access_key_id="lakeadmin", aws_secret_access_key="lakeadmin-secret", region_name="us-east-1")
if "lakehouse" not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
    s3.create_bucket(Bucket="lakehouse")
print("bucket ready: s3://lakehouse")
