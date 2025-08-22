import boto3
import json
import datetime
import requests

def lambda_handler(event, context):
    bucket = 'urban-air-quality-pipeline'
    s3 = boto3.client('s3')
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    api_key = '7965620528e3fa64d6fc14f69a10815dc400beb4f088329ab456844f460eb183'
    headers = {'X-API-Key': api_key}
    
    valid_sensors = [
        {"id": 259, "location": "London Harlington", "param": "o3"},
        {"id": 217, "location": "London Harlington", "param": "pm10"},
        {"id": 261, "location": "London Harlington", "param": "pm25"},
        {"id": 206, "location": "Haringey Roadside", "param": "pm25"},
        {"id": 88, "location": "Haringey Roadside", "param": "no2"},
        {"id": 1304692, "location": "Southwark", "param": "pm25"},
        {"id": 228, "location": "Greenwich", "param": "pm25"},
        {"id": 244, "location": "Bloomsbury", "param": "pm10"},
        {"id": 232, "location": "Bloomsbury", "param": "pm25"},
        {"id": 12626389, "location": "Haringey Priory", "param": "pm10"},
        {"id": 12626392, "location": "Haringey Priory", "param": "pm25"},
        {"id": 233, "location": "Greenwich - Eltham", "param": "no2"},
        {"id": 253, "location": "Greenwich - Eltham", "param": "o3"},
        {"id": 1298329, "location": "Greenwich - Eltham", "param": "pm10"},
        {"id": 228, "location": "Greenwich - Eltham", "param": "pm25"}
    ]
    
    count = 0
    for s in valid_sensors:
        url = f"https://api.openaq.org/v3/sensors/{s['id']}/measurements?limit=1000"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200 or not resp.json().get('results'):
                print(f"❌ No data for sensor {s['id']}")
                continue
            
            key = f"raw/London/{ts}/{s['location'].replace(' ', '_')}_{s['param']}.json"
            s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(resp.json()), ContentType='application/json')
            print(f"✅ Stored: {key}")
            count += 1
        except Exception as e:
            print(f"❌ Failed for {s['id']} — {e}")
            continue

    return {
        'statusCode': 200,
        'body': f"Successfully stored data for {count} sensors"
    }
