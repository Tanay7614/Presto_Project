import fastavro
import psycopg2

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

    # Example SQL query to insert data into Redshift
    sql_query = f"INSERT INTO bid_request ( user_id,  timestamp, auction_id, advertiser_id, ad_type ) VALUES (%s, %s, %s, %s, %s)"
    

    # Execute the SQL query
    cursor.execute(sql_query, (message_body["user_id"] ,message_body["timestamp"] ,message_body["auction_id"],message_body["advertiser_id"],message_body["ad_type"]))
    print("Data_inserted", message_body)

    # Commit the transaction
    conn.commit()
# Open the Avro file for reading
with open('bid_requests.avro', 'rb') as f:
    # Parse the Avro file
    avro_reader = fastavro.reader(f)

    # Iterate through each record in the Avro file
    for record in avro_reader:
        # Print each record
        process_message(record)

# Close communication with the Redshift database
cursor.close()
conn.close()