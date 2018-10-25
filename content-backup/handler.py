import os
import boto3
import string
import urllib.parse

def handler(event, context):
    aws_access_key_id = os.environ['ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['SECRET_ACCESS_KEY']
    source_bucket = os.environ['SOURCE_BUCKET']
    target_bucket = os.environ['TARGET_BUCKET']
    record = event['Records'][0]
    region = record['awsRegion']
    object = record['s3']['object']
    raw_key = object['key']
    #need to fix up the key, which may be uri encoded
    key = urllib.parse.unquote_plus(raw_key)
    size = object['size']
    copy_source = {
        'Bucket': source_bucket,
        'Key': key
    }
    
    client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key)

    client.copy(copy_source, target_bucket, key)

    return 'ok'
