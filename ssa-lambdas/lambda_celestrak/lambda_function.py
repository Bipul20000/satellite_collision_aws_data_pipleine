# lambda_celestrak/lambda_function.py
import json, boto3, urllib.request, os
from datetime import datetime, timezone

s3 = boto3.client('s3')
BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    url = 'https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle'
    
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            raw_text = resp.read().decode('utf-8')
    except Exception as e:
        raise RuntimeError(f"CelesTrak fetch failed: {e}")
    
    lines = [l.strip() for l in raw_text.strip().split('\n') if l.strip()]
    
    satellites = []
    for i in range(0, len(lines) - 2, 3):
        if i + 2 < len(lines):
            name  = lines[i]
            line1 = lines[i+1]
            line2 = lines[i+2]
            if not line1.startswith('1 ') or not line2.startswith('2 '):
                continue
            satellites.append({
                'Satellite_Name':   name,
                'Satellite_Number': line1[2:7].strip(),
                'Classification':   line1[7:8].strip(),
                'Raw_Line_1':       line1,
                'Raw_Line_2':       line2,
                'Inclination':      line2[8:16].strip(),
                'Eccentricity':     line2[26:33].strip(),
                'Mean_Motion':      line2[52:63].strip(),
            })
    
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    key   = f"bronze/satellites/date={today}/active_satellites_raw.json"
    
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(satellites, indent=2),
        ContentType='application/json'
    )
    
    print(f"Stored {len(satellites)} satellites → s3://{BUCKET}/{key}")
    return {'statusCode': 200, 'satellite_count': len(satellites), 'key': key}
