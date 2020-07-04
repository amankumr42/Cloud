import sys
import argparse
# Imports the Google Cloud client library
from google.cloud import storage
import logging

# Instantiates a client
storage_client = storage.Client()

# Logging config
logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger()
logger.setLevel(logging.DEBUG) 

def createBucket(bucket_name, location):  
    list_bucket = []
    list_bucket = list_buckets()
    
    if (bucket_name  in list_bucket ):
        return_message = "Bucket Already Present"
        logger.debug("Bucket Already Present")
    else:
        bucket = storage_client.create_bucket(bucket_name)
        bucket.location = location
        logger.debug("Bucket Created")

def list_buckets():
    returned_bucket_name = []
    buckets = list(storage_client.list_buckets())
    logger.debug("Getting list of the Bucket")
    for bucket in buckets:
        returned_bucket_name.append(bucket.name)

    return returned_bucket_name   


if __name__ == '__main__':
    bucket_name = sys.argv[1]
    location = sys.argv[2]
    createBucket(bucket_name,location)

