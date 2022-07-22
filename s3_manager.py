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
def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
	"""Download a bucket object
	
	:params bucket_name: The target bucket
	:params type: str
	
	:params object_key: The bucket object to get
	:params type: str
	
	:params dest: Optional location where the downloaded file will be stored in your local.
	:params type: str
	
	:returns: The bucket object and downloaded file path object.
	:rtype: tuple
	"""
	bucket = get_bucket(bucket_name)
	params = {'key': object_key}
	if version_id:
		params['VersionId'] = version_id
	bucket_object = bucket.Object(**params)
	dest = Path(f'{dest or ""}')
	file_path = dest.joinpath(PosixPath(object_key).name)
	bucket_object.download_file(f'{file_path}')
	return bucket_object, file_path

# enable bucket versioning
def enable_bucket_versioning(bucket_name):
	"""Enable bucket versioning for the given bucket_name
	"""
	bucket = get_bucket(bucket_name)
	versioned = bucket.Versioning()
	versioned.enable()
	return versioned.status

#delete bucket object and all versions if object is versioned
def delete_bucket_objects(bucket_name, key_prefix=None):
	"""Delete all bucket objects including all versions
	of versioned objects.
	"""
	bucket = get_bucket(bucket_name)
	objects = bucket.object_versions
	if key_prefix:
		objects = objects.filter(Prefix=key_prefix)
	else:
		objects = objects.iterator()
	
	targets = [] # This should be a max of 1000
	for obj in objects:
		targets.append({'Key': obj.object_key,'VersionId': obj.version_id,})
	bucket.delete_objects(Delete={'Objects': targets,'Quiet': True,})
	
	return len(targets)

#delete bucket
def delete_buckets(name=None):
	count = 0
	if name:
		bucket = get_bucket(name)
	if bucket:
		bucket.delete()
		bucket.wait_until_not_exists()
		count += 1
	else:
		count = 0
		client = boto3.resource('s3')
		for bucket in client.buckets.iterator():
			try:
				bucket.delete()
				bucket.wait_until_not_exists()
				count += 1
			except ClientError as err:
				log.warning(f'Bucket {bucket.name}: {err}')

	return count

# Test the functions:
def main():
	new_bucket = input("Enter name of new bucket: ")
	print(f"=============================================================\n1) Creating {new_bucket} ...")
	create_bucket(new_bucket)
	
	print("=============================================================\n2) Listing buckets...")
	list_buckets()
	
	print(f"=============================================================\n3) Getting creation date of {new_bucket} ...")
	c_date = get_bucket(new_bucket).creation_date
	print(f"Creation date of {new_bucket}: ", c_date)
	
	print(f"=============================================================\n4) Uploading new object to {new_bucket} ...")
	#temp_filename = input('Enter name of new file to be uploaded: ')
	tmp_file = create_tempfile()
	print(f"Created new file: {tmp_file}")
	b_obj = create_bucket_object(new_bucket, tmp_file, key_prefix='temp/')
	print(f"Uploaded {tmp_file} in {new_bucket}:\n", b_obj)
	print(f"Object key: ", b_obj.key)
	
	# Delete tempfile in local machine
	tmp_file = Path(tmp_file)
	tmp_file.unlink()
	if tmp_file.exists() == False:
		print('\n\tUploaded file deleted in local machine.')
	print(f"=============================================================\n5) Downloading object from {new_bucket} ...")
	bucket_object_key = b_obj.key
	# Download uploaded file from bucket to local machine
	b_obj, tmp_file = get_bucket_object(new_bucket, bucket_object_key)
	print(f"Downloaded {tmp_file} from {new_bucket}:\n", b_obj)
	print(f"Object key of downloaded file: ", b_obj.key)
	
	print(f"=============================================================\n6) Create new version of {tmp_file} ...")
	print(f"Original version of {tmp_file} :\n", tmp_file.open().read())
	#Edit contents of temp file:
	tmp_file.open(mode='w').write('10' * 500)
	print(f"New version of {tmp_file} :\n", tmp_file.open().read())
	
	print(f"=============================================================\n7) Uploading new version of {tmp_file} to the same object in {new_bucket} ...")
	create_bucket_object(new_bucket, tmp_file.name, key_prefix='temp/')
	latest_v = list(get_bucket(new_bucket).objects.all())
	all_v = list(get_bucket(new_bucket).object_versions.all())
	print(f"Latest versions of all bucket objects:\n", latest_v)
	print(f"All versions of all bucket objects:\n", all_v)	
	
	print(f"=============================================================\n8) Filter bucket contents...")
	print("Creating other objects in bucket...")
	for _ in range(3):
		obj = create_bucket_object(new_bucket, file_path=create_tempfile(), key_prefix='others/')
		print(f'Object {obj.key} created!')
	unfiltered = list(get_bucket(new_bucket).objects.all())
	filtered_temp = list(get_bucket(new_bucket).objects.filter(Prefix='temp/'))
	filtered_others = list(get_bucket(new_bucket).objects.filter(Prefix='others/'))
	print("All objects in bucket (latest versions):\n", unfiltered)
	print("Objects in temp/ :\n", filtered_temp)
	print("Objects in others/ :\n", filtered_others)
	
	print(f"=============================================================\n9) Deleting bucket contents...")
	get_bucket(new_bucket).objects.filter(Prefix='others/').delete()
	files_left = list(get_bucket(new_bucket).object_versions.all())
	print(f"Remaining files in {new_bucket}:\n", files_left)
	print(f"Deleting remaining files in {new_bucket} ...")
	delete_bucket_objects(new_bucket)
	
	print(f"=============================================================\n10) Deleting {new_bucket} ...")
	print("List of buckets before deletion:")
	list_buckets()
	delete_buckets(new_bucket)
	print("List of buckets before deletion:")
	list_buckets()

if __name__ == '__main__':
	main()