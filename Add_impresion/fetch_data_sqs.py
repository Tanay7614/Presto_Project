import boto3
import json
import psycopg2
import time

# Initialize SQS client
sqs = boto3.client('sqs', region_name='ap-south-1')

# Specify your SQS queue URL
queue_url = 'https://sqs.ap-south-1.amazonaws.com/851725480299/advertise_sqs'

   # Connect to Redshift
conn = psycopg2.connect(
        dbname='dev',
        user='awsuser',
        password='Tanay7614',
        host='advertise-cluster.cpf6dhezzpzr.ap-south-1.redshift.amazonaws.com',
        port='5439'
    )

    # Open a cursor to perform database operations
cursor = conn.cursor()

def process_message(message_body):

    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(message_body['timestamp']))

    # Example SQL query to insert data into Redshift
    sql_query = f"INSERT INTO ad_impression ( ad_creative_id, user_id, timestamp,  website) VALUES (%s, %s, %s, %s)"

    # Execute the SQL query
    cursor.execute(sql_query, (message_body["ad_creative_id"],message_body["user_id"] ,timestamp ,message_body["website"]))
    print("Data_inserted", message_body)

    # Commit the transaction
    conn.commit()

    # Close communication with the Redshift database
    # cursor.close()
    # conn.close()


# Receive messages from the SQS queue
while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20  # Long polling to reduce API calls
    )

    # Check if messages are received
    if 'Messages' in response:
        for message in response['Messages']:
            # Extract message body
            message_body = json.loads(message['Body'])

            # Process the message (e.g., insert data into Redshift)
            process_message(message_body)
            print("Data inserted")

            # Delete the message from the SQS queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
    else:
        print("No messages received. Waiting...")

