import json
import random
import time

def generate_ad_impressions():
    ad_creative_ids = ['ad001', 'ad002', 'ad003', 'ad004', 'ad005', 'ad006']
    websites = ['trau.com', 'test.com', 'demo.com', 'native.com']

    ad_impression = {
        'ad_creative_id': random.choice(ad_creative_ids),
        'user_id':  random.randint(1, 2000),
        'timestamp': int(time.time()),
        'website': random.choice(websites)
    }

    return ad_impression

# Specify the filename to write
filename = 'ad_impressions.json'

# Open the file in write mode
with open(filename, 'w') as file:
    while True:
        # Generate ad impression data
        ad_impression_data = generate_ad_impressions()

        # Write ad impression data to the file
        json.dump(ad_impression_data, file)
        file.write('\n')  # Write a newline to separate each JSON object

        # Wait for a certain interval before generating the next ad impression
        time.sleep(1)  # Adjust the interval as needed
