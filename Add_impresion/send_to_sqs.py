import boto3
import json, random

# Specify the AWS region and your SQS queue URL
region_name = 'ap-south-1'
queue_url = 'https://sqs.ap-south-1.amazonaws.com/851725480299/advertise_sqs'

# Initialize the SQS client
sqs_client = boto3.client('sqs', region_name=region_name)

def read_data_from_json(filename):
    with open(filename, 'r') as file:
        data = file.readlines()
    return data

# File containing JSON data
json_filename = 'ad_impressions.json'

# Continuously send messages to the SQS queue
while True:
    data_lines = read_data_from_json(json_filename)
    for line in data_lines:
        # Parse JSON line to a dictionary
        message_body = json.loads(line)

        # Convert message to JSON format
        message_body_json = json.dumps(message_body)

        # Send the message to the SQS queue
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body_json
        )

# Print the response
        print("Message sent. Response:", message_body)
