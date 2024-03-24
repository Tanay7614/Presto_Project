import csv
import random
import time

# Function to generate mock CSV data
def generate_csv_data(num_rows):
    headers = ['timestamp', 'user_id', 'ad_campaign_id', 'conversion_type']
    data = []

    for _ in range(num_rows):
        timestamp = int(time.time())
        user_id = random.randint(1, 1000)
        ad_campaign_id = random.randint(1, 10)
        conversion_type = random.choice(['signup', 'purchase', 'click'])

        row = [timestamp, user_id, ad_campaign_id, conversion_type]
        data.append(row)

    return headers, data

# Function to write CSV data to a file
def write_csv_file(filename, headers, data):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)

try:
    num_rows = 200
    headers, data = generate_csv_data(num_rows)

    # Write CSV data to a file
    filename = 'click_conversion_data.csv'
    write_csv_file(filename, headers, data)

    print(f"Generated {num_rows} rows of CSV data and saved to {filename}")

except Exception as e:
    print("An error occurred:", e)
