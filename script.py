import boto3
from dotenv import load_dotenv
import os
import json
import math
import logging
import coloredlogs

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG')
coloredlogs.install(
    logger=logger,
    fmt='%(asctime)s.%(msecs)03d %(filename)s:%(lineno)d %(levelname)s %(message)s'
)

# load the .env file
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_access_secret_key = os.getenv('AWS_ACCESS_SECRET_KEY')
aws_region = os.getenv('AWS_REGION')
aws_sqs_queue_name = os.getenv('AWS_SQS_QUEUE_NAME')

client = boto3.resource('sqs', region_name=aws_region,
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_access_secret_key)

queue = client.get_queue_by_name(QueueName=aws_sqs_queue_name)

f = open('./user.json', 'r')
data = json.load(f)

messages_list = []
for index, user in enumerate(data["Users:"]):
    messages_list.append({"Id":str(index),"MessageBody": json.dumps(user)})

# max no of items that can be sent to a SQS queue in a single request is 10
max_batch_size = 10
no_of_batches = math.ceil(len(messages_list)/max_batch_size)

logging.info(f"Total no of messages to be sent to the SQS Queue: {len(messages_list)}")
logging.info(f"No of batches required: {no_of_batches}")

start_index = 0

for i in range(no_of_batches):
    end_index = start_index + max_batch_size - 1
    pipeline_items = messages_list[start_index:end_index+1]

    logging.info(f"Processing batch no. {i+1}, messages count: {len(pipeline_items)}")

    # send multiple messages to the queue in a single call
    response = queue.send_messages(Entries=[item for item in pipeline_items])

    start_index = end_index + 1