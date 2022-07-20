# s3_manager by: ROSELE

import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
level=logging.INFO, format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s')
log = logging.getLogger()

# create a bucket
def create_bucket(name, region=None):
    region = region or 'us-east-2'
    client = boto3.client('s3', region_name=region)
    params = {'Bucket': name, 'CreateBucketConfiguration': {'LocationConstraint': region,}}

    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False


# list buckets
def list_buckets():
    s3 = boto3.resource('s3')
    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1
    print(f'Found {count} buckets!')


# get a bucket
def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return


# create bucket objects
def create_tempfile(file_name=None, content=None, size=300):
    """Create a temporary text file"""
    filename = f'{file_name or uuid.uuid4().hex}.txt'
    with open(filename, 'w') as f:
        f.write(f'{(content or "0") * size}')
    return filename

def create_bucket_object(bucket_name, file_path, key_prefix=None):
	"""Create a bucket object
	:params bucket_name: The target bucket
	:params type: str
	:params file_path: The path to the file to be uploaded to the bucket.
	:params type: str
	:params key_prefix: Optional prefix to set in the bucket for the file.
	:params type: str
	"""
	bucket = get_bucket(bucket_name)
	dest = f'{key_prefix or ""}{file_path}'
	bucket_object = bucket.Object(dest)
	bucket_object.upload_file(Filename=file_path)
	return bucket_object


# get bucket object
