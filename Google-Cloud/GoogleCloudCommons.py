import argparse
from datetime import datetime
# Imports the Google Cloud client library
from google.cloud import storage
import logging

# Instantiates a client
storage_client = storage.Client()

# Logging config
logging.basicConfig(level=logging.DEBUG)
logger=logging.getLogger()
logger.setLevel(logging.DEBUG) 

# Setting up the Argumnet variable
parser = argparse.ArgumentParser(description="Setting up the cloud storage and uploading the file")
parser.add_argument("-b","--bucket_name", type= str , help= "name of the bucket")
parser.add_argument("-l","--bucket_location", type= str, help= "location of the bucket")
parser.add_argument("-f","--file_name", type= str , help= "Initlaize name of the file")
parser.add_argument("-s","--source_path", type= str , help= "Initlaize source path of the file")
parser.add_argument("-tf", "--cloud_file_name" , type = str , help = "File name to be saved in cloud storage")
args = parser.parse_args()

def createBucket(bucket_name, bucket_location):  
    list_bucket = []
    list_bucket = list_buckets()
    
    if (bucket_name  in list_bucket ):
        logger.debug("Bucket Already Present")
    else:
        bucket = storage_client.create_bucket(bucket_name)
        bucket.location = bucket_location
        logger.debug("Bucket Created")

def list_buckets():
    returned_bucket_name = []
    buckets = list(storage_client.list_buckets())
    logger.debug("Getting list of the Bucket")
    for bucket in buckets:
        returned_bucket_name.append(bucket.name)

    return returned_bucket_name   

def upload_object(bucket_name, source_file_name, destination_blob_name):

    logger.debug("Uploading file to cloud storage")

    bucket = storage_client.bucket(bucket_name)
    print(create_sub_folder())
    blob = bucket.blob(create_sub_folder() + "/" + "file_name")
    blob.upload_from_string('')

    blob.upload_from_filename(source_file_name)
    logger.debug("Upload is complete")

def create_sub_folder():
    logger.debug("Create sub directory in google cloud")
    current_data = datetime.today().strftime('%Y-%m-%d')
    return current_data

if __name__ == '__main__':
    createBucket(args.bucket_name,args.bucket_location)
    upload_object(args.bucket_name,args.cloud_file_name,args.source_path)
