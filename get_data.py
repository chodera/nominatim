import requests
import psycopg2
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

def reverse_geocode(lat, lon):
    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        address = data.get('address', 'Not found')
        print(f"Processed {address}")
        return address
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# PostgreSQL connection details
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME') 
DB_USER = os.getenv('DB_USER') 
DB_PASSWORD = os.getenv('DB_PASSWORD') 

# Connect to the PostgreSQL database
print("Connecting to the database...")
conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()

# Fetch coordinates from the database
print("Fetching coordinates from the database...")
cursor.execute(
    """
    with

    base as (

    select distinct
        plants.latitude,
        plants.longitude,
        row_number() over (order by plants.latitude, plants.longitude) as row_number
    from raw_mundraub.plants
    left join raw_nominatim.geocoding_results on
        plants.longitude :: varchar = geocoding_results.longitude
        and plants.latitude :: varchar = geocoding_results.latitude
    where geocoding_results.longitude is null

    )

    select
       latitude,
       longitude
    from base
    order by row_number;
    """
)
coordinates = cursor.fetchall()

# Output file path
output_file = 'geocoding_results.json'

# Perform reverse geocoding and store results in a JSON file
total_coordinates = len(coordinates)
print(f"Performing reverse geocoding for {total_coordinates} coordinates...")
with open(output_file, 'w') as file:
    for i, (lat, lon) in enumerate(coordinates, 1):
        print(f"Processing coordinate {i}/{total_coordinates}")
        address = reverse_geocode(lat, lon)

        # Prepare the data as a dictionary
        data = {
            "latitude": lat,
            "longitude": lon,
            "address": address
        }

        # Write the data as a JSON line
        file.write(json.dumps(data) + '\n')

        time.sleep(0.5)  # To avoid hitting the Nominatim rate limit

# Close the database connection
print("Closing the database connection.")
cursor.close()
conn.close()

print(f"Results saved to {output_file}")
