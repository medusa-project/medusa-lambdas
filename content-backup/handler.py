import os
import boto3
import uuid
import urllib.parse
import json
import time

# noinspection PyBroadException
def lambda_handler(event, context):
  params = lambda_params(event)
  run_uuid = str(uuid.uuid4())
  send_message('start', run_uuid, params)
  try:
    copy_object(params)
    send_message('end', run_uuid, params)
  except:
    send_message('error', run_uuid, params)
  return 'ok'

def lambda_params(event):
  record = event['Records'][0]
  source_region = record['awsRegion']
  s3_object = record['s3']['object']
  raw_key = s3_object['key']
  #need to fix up the key, which may be uri encoded
  object_key = urllib.parse.unquote_plus(raw_key)
  size = s3_object['size']
  return {
    'aws_access_key_id': os.environ['ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['SECRET_ACCESS_KEY'],
    'source_bucket': os.environ['SOURCE_BUCKET'],
    'source_region': source_region,
    'target_bucket': os.environ['TARGET_BUCKET'],
    'target_region': os.environ['TARGET_REGION'],
    'queue_url': os.environ['SQS_URL'],
    'object_key': object_key,
    'size': size
  }

def send_message(event_name, run_uuid, params):
  sqs_client = boto3.client('sqs', aws_access_key_id=params['aws_access_key_id'],
                            aws_secret_access_key=params['aws_secret_access_key'])
  sqs_client.send_message(QueueURL=params['QueueUrl'],
                          MessageBody=message_body(event_name, run_uuid, params['object_key']))

def message_body(event_name, run_uuid, object_key):
  current_time = int(time.time())
  message = {
    'event': event_name,
    'run_uuid': run_uuid,
    'object_key': object_key,
    'time': current_time
  }
  return json.dumps(message)

def copy_object(params):
  copy_source = {
    'Bucket': params['source_bucket'],
    'Key': params['object_key']
  }
  client = boto3.client(
    's3',
    region_name=params['target_region'],
    aws_access_key_id=params['aws_access_key_id'],
    aws_secret_access_key=params['aws_secret_access_key'])
  source_client = boto3.client(
    's3',
    region_name=params['source_region'],
    aws_access_key_id=params['aws_access_key_id'],
    aws_secret_access_key=params['aws_secret_access_key'])
  client.copy(copy_source, params['target_bucket'], params['object_key'], SourceClient=source_client)
