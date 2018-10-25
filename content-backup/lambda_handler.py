import os
import boto3
import uuid
import urllib.parse
import json
import time
import copier

# noinspection PyBroadException
def handler(event, context):
  params = lambda_params(event)
  run_uuid = str(uuid.uuid4())
  send_message('start', run_uuid, params)
  if params['size'] < 200 * (1024 ** 3):
    try:
      copier.copy_object(params)
      send_message('end', run_uuid, params)
    except:
      send_message('error', run_uuid, params)
    return 'ok'
  else:
    send_message('too_big', run_uuid, params)

def lambda_params(event):
  record = event['Records'][0]
  source_region = record['awsRegion']
  s3_object = record['s3']['object']
  raw_key = s3_object['key']
  # need to fix up the key, which may be uri encoded
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
  sqs_client.send_message(QueueUrl=params['queue_url'],
                          MessageBody=message_body(event_name, run_uuid, params['object_key']))

def message_body(event_name, run_uuid, object_key):
  current_time = time.time()
  message = {
    'event': event_name,
    'run_uuid': run_uuid,
    'object_key': object_key,
    'time': current_time
  }
  return json.dumps(message)


