# lambda_noaa/lambda_function.py
import json, boto3, urllib.request, os
from datetime import datetime, timezone

s3     = boto3.client('s3')
BUCKET = os.environ['S3_BUCKET']

NOAA_URLS = {
    'short': 'https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json',
    'long':  'https://services.swpc.noaa.gov/json/goes/secondary/xrays-1-day.json',
}

def fetch(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))

def lambda_handler(event, context):
    today   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    records = []
    
    for band, url in NOAA_URLS.items():
        try:
            data = fetch(url)
            for r in data:
                records.append({
                    'time_tag':    r.get('time_tag'),
                    'satellite':   r.get('satellite'),
                    'flux':        r.get('flux'),
                    'energy':      r.get('energy'),
                    'band':        band,
                    'fetched_date': today,
                })
        except Exception as e:
            print(f"NOAA {band} fetch failed: {e}")
    
    # Classify solar flare level from peak flux
    peak_flux = max((r['flux'] for r in records if r['flux']), default=0)
    if   peak_flux >= 1e-4: flare_class = 'X'
    elif peak_flux >= 1e-5: flare_class = 'M'
    elif peak_flux >= 1e-6: flare_class = 'C'
    elif peak_flux >= 1e-7: flare_class = 'B'
    else:                   flare_class = 'A'
    
    payload = {
        'records':     records,
        'peak_flux':   peak_flux,
        'flare_class': flare_class,
        'record_count': len(records),
        'fetched_date': today,
    }
    
    key = f"bronze/weather/date={today}/noaa_goes_raw.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(payload, indent=2),
        ContentType='application/json'
    )
    
    print(f"Stored {len(records)} flux readings (peak: {flare_class}-class) → s3://{BUCKET}/{key}")
    return {'statusCode': 200, 'record_count': len(records), 'flare_class': flare_class, 'key': key}
