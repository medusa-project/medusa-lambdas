import boto3

#params should be a dictionary that has everything expected in this method:
# source/target bucket and region
# aws credentials
# object key
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