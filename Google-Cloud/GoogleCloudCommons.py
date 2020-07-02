import sys
# Imports the Google Cloud client library
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()

def createBucket(bucket_name, location):  
    bucket = storage_client.create_bucket(bucket_name)
    bucket.location = location

    return "Created Bucket"

def list_buckets():
    returned_bucket_name = []
    buckets = storage_client.list_buckets()
    
    for bucket in buckets:
        returned_bucket_name.append(bucket.format(bucket.name))

    return returned_bucket_name   



