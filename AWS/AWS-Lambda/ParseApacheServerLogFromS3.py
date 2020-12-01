import json
import logging
import boto3
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')

HOST = r'^(?P<host>.*?)'
SPACE = r'\s'
IDENTITY = r'\S+'
USER = r'\S+'
TIME = r'(?P<time>\[.*?\])'
REQUEST = r'\"(?P<request>.*?)\"'
STATUS = r'(?P<status>\d{3})'
SIZE = r'(?P<size>\S+)'

REGEX = HOST+SPACE+IDENTITY+SPACE+USER+SPACE+TIME+SPACE+REQUEST+SPACE+STATUS+SPACE+SIZE+SPACE

def parser(log_line: object) -> object:
    match = re.search(REGEX,log_line)
    return ((match.group('host'),match.group('time'),match.group('request') ,match.group('status') ,match.group('size')))

def lambda_handler(event, context):
    if event:
        file_key = event['Records'][0]['s3']['object']['key']
        # get the object
        obj = s3.get_object(Bucket=	"sample-bucket-amank-1", Key=file_key)
        lines = obj['Body'].read().split(b'\n')
        for r in lines:
            logger.info(parser(r.decode()))
    return "Success"
