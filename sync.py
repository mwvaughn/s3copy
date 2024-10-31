import boto3
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tqdm import tqdm
import logging
import re
from botocore.config import Config
import urllib3

def parse_s3_url(s3_url):
    """Parse s3://bucket-name/prefix into bucket and prefix."""
    match = re.match(r's3://([^/]+)/?(.+)?', s3_url)
    if not match:
        raise ValueError("Invalid S3 URL format. Expected: s3://bucket-name/optional-prefix")
    
    bucket = match.group(1)
    prefix = match.group(2) or ''
    # Ensure prefix doesn't start with / but does end with / if not empty
    prefix = prefix.strip('/')
    if prefix:
        prefix = prefix + '/'
    
    return bucket, prefix

def download_file(bucket_name, s3_key, local_path, s3_client):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(bucket_name, s3_key, local_path)
        return True
    except Exception as e:
        logging.error(f"Error downloading {s3_key}: {str(e)}")
        return False

def sync_s3_bucket(s3_url, local_dir, aws_profile=None, max_workers=10):
    # Parse S3 URL
    bucket_name, prefix = parse_s3_url(s3_url)
    
    # Configure boto3 client with appropriate connection pool settings
    max_pool_connections = max_workers + 10  # Add some buffer for list operations
    config = Config(
        max_pool_connections=max_pool_connections,
        retries=dict(max_attempts=3),  # Add retry configuration
        connect_timeout=60,            # Increase timeouts for large files
        read_timeout=60
    )
    
    # If no profile is specified, boto3 will use instance profile credentials
    session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
    
    # Create S3 client with the custom config
    s3_client = session.client('s3', config=config)
    
    # Configure the underlying urllib3 pool manager
    urllib3_pool = urllib3.PoolManager(
        maxsize=max_pool_connections,
        retries=urllib3.Retry(3),
        timeout=urllib3.Timeout(connect=60, read=60)
    )
    
    logging.basicConfig(
        filename='s3_sync.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    
    paginator = s3_client.get_paginator('list_objects_v2')
    all_objects = []
    
    # Use the prefix in the list operation
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            all_objects.extend(page['Contents'])
    
    if not all_objects:
        print(f"No objects found in {s3_url}")
        return
    
    print(f"Found {len(all_objects)} objects to sync")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for obj in all_objects:
            s3_key = obj['Key']
            
            # Remove the prefix from the local path if it exists
            if prefix:
                local_key = s3_key[len(prefix):]
            else:
                local_key = s3_key
                
            local_path = str(local_dir / local_key)
            futures.append(
                executor.submit(download_file, bucket_name, s3_key, local_path, s3_client)
            )
        
        with tqdm(total=len(futures), desc="Syncing files") as pbar:
            for future in futures:
                future.result()
                pbar.update(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("s3_url", help="S3 URL (e.g., s3://bucket-name/prefix)")
    parser.add_argument("local_dir", help="Local directory to sync to")
    parser.add_argument("--profile", help="AWS profile name (optional, uses instance profile if not specified)")
    parser.add_argument("--workers", type=int, default=10, help="Number of download workers")
    args = parser.parse_args()
    
    sync_s3_bucket(args.s3_url, args.local_dir, args.profile, args.workers)

