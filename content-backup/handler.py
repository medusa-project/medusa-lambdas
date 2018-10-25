import os
import boto3
import uuid
import urllib.parse


def lambda_handler(event, context):
  aws_access_key_id = os.environ['ACCESS_KEY_ID']
  aws_secret_access_key = os.environ['SECRET_ACCESS_KEY']
  source_bucket = os.environ['SOURCE_BUCKET']
  target_bucket = os.environ['TARGET_BUCKET']
  target_region = os.environ['TARGET_REGION']
  queue_url = os.environ['SQS_URL']
  record = event['Records'][0]
  source_region = record['awsRegion']
  s3_object = record['s3']['object']
  raw_key = s3_object['key']
  #need to fix up the key, which may be uri encoded
  object_key = urllib.parse.unquote_plus(raw_key)
  size = s3_object['size']
  sqs_client = boto3.client('sqs', aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
  runtime_uuid = str(uuid.uuid4())
  sqs_client.send_message(QueueUrl=queue_url,
                          MessageBody=runtime_uuid)
  copy_object(aws_access_key_id, aws_secret_access_key, object_key, source_bucket, source_region, target_bucket, target_region)

  return 'ok'


def copy_object(aws_access_key_id, aws_secret_access_key, object_key, source_bucket, source_region, target_bucket,
                target_region):
  copy_source = {
    'Bucket': source_bucket,
    'Key': object_key
  }
  client = boto3.client(
    's3',
    region_name=target_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)
  source_client = boto3.client(
    's3',
    region_name=source_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)
  client.copy(copy_source, target_bucket, object_key, SourceClient=source_client)
