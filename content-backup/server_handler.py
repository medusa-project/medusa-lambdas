#!/usr/bin/env python3
#  The idea:
# cron runnable script that does the following:
#
# takes messages off of the SQS queue
# for each message updates the database appropriately, logs, and deletes the message
#
# once the messages are all off, move stuff from the active table to the archive table or log as appropriate
# then look for copies that need to be done and do some number of them (we may want to limit this
# so as to get back to clearing the queue). Possibly thread things, as these copies should be easy to
# do in parallel
#
# Then repeat
#
# Presumably use with a bash script that makes sure that only one of these is running at a time, plus to
# set up the environment variables. Plus added a thing to actually call the method here that needs to be
# run, so this is just the code, not the script entry point.
#
# May want to add logs/db to the backup procedure
# Need to figure out python db stuff, and decide whether to connect to main database or to use something like sqlite.
# Need to figure out python log stuff - maybe have standard log and error log?

import logging
import logging.config
import logging.handlers
import os
import os.path
import boto3
import json
import sqlite3
import time
import copier

def process():
  ensure_db()
  setup_loggers()
  default_logger().info('a message')
  error_logger().info('an error')
  process_queue()
  cleanup_database()
  process_copy()

def process_queue():
  session = aws_session()
  queue_url = aws_params()['queue_url']
  sqs = session.resource('sqs', region_name='us-east-2')
  queue = sqs.Queue(url=queue_url)
  connection = db_connection()
  more = process_batch(queue, connection)
  while more:
    more = process_batch(queue, connection)

def process_batch(queue, connection):
  messages = queue.receive_messages(MaxNumberOfMessages=10)
  if len(messages) == 0:
    return False
  else:
    for message in messages:
      process_message(message.body, connection)
      message.delete()
    return True

def process_message(body, connection):
  message = json.loads(body)
  update_database(message, connection)

def update_database(message, connection):
  event = message['event']
  run_uuid = message['run_uuid']
  object_key = message['object_key']
  timestamp = message['time']
  if event == 'start':
    handle_start(run_uuid, object_key, timestamp, connection)
  elif event == 'end':
    handle_end(run_uuid, object_key, timestamp, connection)
  elif event == 'error':
    handle_error(run_uuid, object_key, timestamp, connection)
  elif event == 'too_big':
    handle_too_big(run_uuid, object_key, timestamp, connection)
  else:
    error_logger().info("Unknown event received:", event)
    raise ValueError('Unknown event type received from queue')

def handle_start(run_uuid, object_key, timestamp, connection):
  if record_exists(run_uuid, connection):
    connection.execute('UPDATE backups SET start_time=? WHERE run_uuid=?', (timestamp, run_uuid))
  else:
    connection.execute('INSERT INTO backups (run_uuid, object_key, start_time) VALUES (?,?,?)',
                       (run_uuid, object_key, timestamp))
  default_logger().info("Start: ", timestamp, run_uuid, object_key)
  connection.commit()

def handle_end(run_uuid, object_key, timestamp, connection):
  if record_exists(run_uuid, connection):
    connection.execute('UPDATE backups SET end_time=? WHERE run_uuid=?', (timestamp, run_uuid))
  else:
    connection.execute('INSERT INTO backups (run_uuid, object_key, end_time) VALUES (?,?,?)',
                       (run_uuid, object_key, timestamp))
  default_logger().info("End: ", timestamp, run_uuid, object_key)
  connection.commit()

def handle_error(run_uuid, object_key, timestamp, connection):
  error_logger().info("Error:", timestamp, run_uuid, object_key)
  print("Error:", run_uuid, object_key)

def handle_too_big(run_uuid, object_key, timestamp, connection):
  print("Too big:", run_uuid, object_key)
  default_logger().info("Too big: ", timestamp, run_uuid, object_key)

def record_exists(run_uuid, connection):
  cursor = connection.execute("SELECT 1 FROM backups WHERE run_uuid=:run_uuid", {'run_uuid': run_uuid})
  return cursor.fetchone()

def cleanup_database():
  connection = db_connection()
  connection.execute('BEGIN TRANSACTION')
  connection.execute('''
    INSERT INTO archived_backups (run_uuid, object_key, start_time, end_time)
    SELECT run_uuid, object_key, start_time, end_time FROM backups
      WHERE start_time IS NOT NULL AND end_time IS NOT NULL;
    ''')
  connection.execute('''
    DELETE FROM backups WHERE start_time IS NOT NULL AND end_time IS NOT NULL;
    ''')
  connection.execute('COMMIT')
  connection.commit()

def process_copy():
  copy_start_time = time.time()
  connection = db_connection()
  while True and ((time.time() - 1800) < copy_start_time):
    cursor = connection.execute('SELECT run_uuid, object_key FROM backups WHERE end_time IS NULL AND start_time < :max_start_time ORDER BY start_time ASC LIMIT 1',
                                {'max_start_time': copy_start_time - 3600})
    record = cursor.fetchone()
    if record:
      (run_uuid, object_key) = record
      now = time.time()
      copy_logger().info('Copy Start:', now, run_uuid, object_key)
      do_copy(object_key)
      connection.execute('UPDATE backups SET end_time=? WHERE run_uuid=?', (now, run_uuid))
      connection.commit()
      copy_logger().info('Copy End:', now, run_uuid, object_key)
    else:
      return

def do_copy(object_key):
  print ('Copying: ', object_key)
  params = aws_params()
  params['object_key'] = object_key
  copier.copy_object(params)

def setup_loggers():
  configure_logger(error_logger(), 'log/errors.log')
  configure_logger(default_logger(), 'log/default.log')
  configure_logger(copy_logger(), 'log/copy.log')

def configure_logger(logger, file):
  logger.setLevel(logging.INFO)
  handler = logging.handlers.WatchedFileHandler(file)
  handler.setFormatter(logging.Formatter('%(asctime)s|%(levelname)s|%(message)s'))
  logger.addHandler(handler)

def error_logger():
  return logging.getLogger('error')

def default_logger():
  return logging.getLogger('default')

def copy_logger():
  return logging.getLogger('copy')

def aws_params():
  return {
    'aws_access_key_id': os.environ['ACCESS_KEY_ID'],
    'aws_secret_access_key': os.environ['SECRET_ACCESS_KEY'],
    'source_bucket': os.environ['SOURCE_BUCKET'],
    'source_region': os.environ['SOURCE_REGION'],
    'target_bucket': os.environ['TARGET_BUCKET'],
    'target_region': os.environ['TARGET_REGION'],
    'queue_url': os.environ['SQS_URL'],
    'queue_region': os.environ['SQS_QUEUE_REGION']
  }

def aws_session():
  params = aws_params()
  return boto3.Session(aws_access_key_id=params['aws_access_key_id'],
                       aws_secret_access_key=params['aws_secret_access_key'])

def db_name():
  return 'backup.db'

def db_connection():
  return sqlite3.connect(db_name())

def db_exists():
  return os.path.isfile(db_name())

def ensure_db():
  if not db_exists():
    connection = db_connection()
    sql = '''
      CREATE TABLE backups(run_uuid text PRIMARY KEY, object_key text, start_time double, end_time double);
      CREATE TABLE archived_backups(run_uuid text PRIMARY KEY, object_key text, start_time double, end_time double);
      CREATE INDEX b_object_key ON backups(object_key);
      CREATE INDEX b_start_time ON backups(start_time);
      CREATE INDEX b_end_time ON backups(end_time);
      CREATE INDEX ab_object_key ON archived_backups(object_key);
      CREATE INDEX ab_start_time ON archived_backups(start_time);
      CREATE INDEX ab_end_time ON archived_backups(end_time); 
    '''
    connection.executescript(sql)
    connection.commit()
