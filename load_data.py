import json
import singer

addresses = [] 

with open('/Users/davidwitkowski/Development/projects/nominatim/geocoding_results.json') as f:
    json_list = [json.loads(line) for line in f.readlines()]
    for i in range(0, len(json_list)):

        try: 
            lat = json_list[i].get('latitude')
            lon = json_list[i].get('longitude')
            data = {
                'latitude': lat,
                'longitude': lon,
                'shop': json_list[i]['address'].get('shop'),
                'house_number': json_list[i]['address'].get('house_number'),
                'road': json_list[i]['address'].get('road'),
                'village': json_list[i]['address'].get('village'),
                'municipality': json_list[i]['address'].get('municipality'),
                'city': json_list[i]['address'].get('city'),
                'city_district': json_list[i]['address'].get('city_district'),
                'suburb': json_list[i]['address'].get('suburb'),
                'town': json_list[i]['address'].get('town'),
                'county': json_list[i]['address'].get('county'),
                'state': json_list[i]['address'].get('state'),
                'region': json_list[i]['address'].get('region'),
                'postcode': json_list[i]['address'].get('postcode'),
                'country': json_list[i]['address'].get('country'),
                'country_code': json_list[i]['address'].get('country_code'),
            }

        except Exception as e: 
            print(f'Error for {lat}:  {e}')
        addresses.append(data)
 
singer.write_schema(
    'geocoding_results', 
    {'properties': {
        'latitude': {'type': 'string'},
        'longitude': {'type': 'string'},
        "shop": {"type": "string"},
        "house_number": {"type": "string"},
        "road": {"type": "string"},
        "village": {"type": "string"},
        "municipality": {"type": "string"},
        "city": {"type": "string"},
        "city_district": {"type": "string"},
        "suburb": {"type": "string"},
        "town": {"type": "string"},
        "county": {"type": "string"},
        "state": {"type": "string"},
        "region": {"type": "string"},
        "postcode": {"type": "string"},
        "country": {"type": "string"},
        "country_code": {"type": "string"},
        }},
    key_properties=['latitude', 'longitude']
    )

singer.write_records('geocoding_results', addresses)
