# lambda_nasa/lambda_function.py
import json, boto3, urllib.request, urllib.parse, os
from datetime import datetime, timedelta, timezone

s3      = boto3.client('s3')
sm      = boto3.client('secretsmanager', region_name='ap-south-1')
BUCKET  = os.environ['S3_BUCKET']

def get_nasa_key():
    secret = sm.get_secret_value(SecretId='ssa/nasa_api_key')
    return secret['SecretString']

def lambda_handler(event, context):
    api_key    = get_nasa_key()
    today      = datetime.now(timezone.utc)
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date   = today.strftime('%Y-%m-%d')
    
    url = (
        f"https://api.nasa.gov/neo/rest/v1/feed"
        f"?start_date={start_date}&end_date={end_date}&api_key={api_key}"
    )
    
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        raise RuntimeError(f"NASA NeoWs fetch failed: {e}")
    
    asteroids = []
    for date_key, neo_list in data.get('near_earth_objects', {}).items():
        for ast in neo_list:
            approach = ast['close_approach_data'][0] if ast['close_approach_data'] else {}
            asteroids.append({
                'id':                       ast.get('id'),
                'name':                     ast.get('name'),
                'is_potentially_hazardous': ast.get('is_potentially_hazardous_asteroid', False),
                'diameter_min_m':           ast['estimated_diameter']['meters']['estimated_diameter_min'],
                'diameter_max_m':           ast['estimated_diameter']['meters']['estimated_diameter_max'],
                'close_approach_date':      approach.get('close_approach_date'),
                'relative_velocity_kph':    float(approach.get('relative_velocity', {}).get('kilometers_per_hour', 0)),
                'miss_distance_km':         float(approach.get('miss_distance', {}).get('kilometers', 0)),
                'orbiting_body':            approach.get('orbiting_body'),
                'fetched_date':             end_date,
            })
    
    key = f"bronze/asteroids/date={end_date}/nasa_neows_raw.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(asteroids, indent=2),
        ContentType='application/json'
    )
    
    print(f"Stored {len(asteroids)} asteroids → s3://{BUCKET}/{key}")
    return {'statusCode': 200, 'asteroid_count': len(asteroids), 'key': key}
