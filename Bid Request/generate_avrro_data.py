from avro import schema, datafile, io
import random
import time

# Define the Avro schema
avro_schema_str = """
{
  "type": "record",
  "name": "BidRequest",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "auction_id", "type": "string"},
    {"name": "advertiser_id", "type": "string"},
    {"name": "ad_type", "type": "string"}
  ]
}
"""

# Parse the Avro schema
avro_schema = schema.Parse(avro_schema_str)

# Initialize Avro data file writer
writer = datafile.DataFileWriter(open("bid_requests.avro", "wb"), io.DatumWriter(), avro_schema)

# List of possible values for advertiser_id and ad_type
advertiser_ids = ['advertiser001', 'advertiser002', 'advertiser003']
ad_types = ['banner', 'video', 'native']

# Generate and write Avro records
for _ in range(100):  # Generate 100 records
    bid_request = {
        'user_id': str(random.randint(1, 2000)),
        'timestamp': str(int(time.time())),
        'auction_id': 'auction_' + str(random.randint(1, 100)),
        'advertiser_id': random.choice(advertiser_ids),
        'ad_type': random.choice(ad_types)
    }
    writer.append(bid_request)

# Close the writer
writer.close()

print("Avro data file generated successfully.")
