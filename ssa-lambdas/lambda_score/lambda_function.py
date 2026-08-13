# lambda_score/lambda_function.py
from decimal import Decimal
import json, boto3, os
from datetime import datetime, timezone

athena  = boto3.client('athena', region_name='ap-south-1')
sns     = boto3.client('sns',    region_name='ap-south-1')
dynamo  = boto3.resource('dynamodb', region_name='ap-south-1')

BUCKET    = os.environ['S3_BUCKET']
TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
WORKGROUP = 'ssa-workgroup'

def run_query(sql):
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
    )
    qid = resp['QueryExecutionId']

    # Poll until done
    import time
    while True:
        status = athena.get_query_execution(
            QueryExecutionId=qid
        )['QueryExecution']['Status']['State']
        if status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(3)

    if status != 'SUCCEEDED':
        raise RuntimeError(f"Query {qid} {status}")

    rows = athena.get_query_results(
        QueryExecutionId=qid
    )['ResultSet']['Rows']
    return rows

def lambda_handler(event, context):
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # 1. Get risk summary from gold table
    rows = run_query(f"""
        SELECT risk_level, COUNT(*) as cnt, ROUND(MIN(distance_km), 4) as closest_km
        FROM ssa_gold.fact_collision_risks
        WHERE run_date = '{today}'
        GROUP BY risk_level
        ORDER BY closest_km ASC
    """)

    summary = {}
    for row in rows[1:]:  # skip header
        level   = row['Data'][0]['VarCharValue']
        count   = int(row['Data'][1]['VarCharValue'])
        closest = float(row['Data'][2]['VarCharValue'])
        summary[level] = {'count': count, 'closest_km': closest}

    critical_count = summary.get('CRITICAL', {}).get('count', 0)
    caution_count  = summary.get('CAUTION',  {}).get('count', 0)
    safe_count     = summary.get('SAFE',     {}).get('count', 0)

    print(f"Summary: CRITICAL={critical_count}, CAUTION={caution_count}, SAFE={safe_count}")

    # 2. Get top 5 critical pairs
    critical_rows = run_query(f"""
        SELECT target_sat, threat_sat, distance_km
        FROM ssa_gold.fact_collision_risks
        WHERE run_date = '{today}' AND risk_level = 'CRITICAL'
        ORDER BY distance_km ASC
        LIMIT 5
    """)

    top_pairs = []
    for row in critical_rows[1:]:
        top_pairs.append({
            'target': row['Data'][0]['VarCharValue'],
            'threat': row['Data'][1]['VarCharValue'],
            'distance_km': float(row['Data'][2]['VarCharValue'])
        })

    # 3. Write to DynamoDB
    table = dynamo.Table('ConjunctionRisks')
    table.put_item(Item={
    	'run_date':       today,
    	'distance_km':    Decimal(str(summary.get('CRITICAL', {}).get('closest_km', 999))),
    	'critical_count': critical_count,
    	'caution_count':  caution_count,
    	'safe_count':     safe_count,
    	'top_pairs':      json.dumps(top_pairs),
	})
    # 4. Fire SNS alert if CRITICAL > 0
    if critical_count > 0:
        message = f"""
🚨 SSA CRITICAL ALERT — {today}

{critical_count} CRITICAL conjunction events detected!
{caution_count} CAUTION events
{safe_count} SAFE events

Top Critical Pairs:
"""
        for p in top_pairs:
            message += f"  • {p['target']} ↔ {p['threat']}: {p['distance_km']} km\n"

        message += "\nReview dashboard for full details."

        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"🚨 SSA Alert: {critical_count} Critical Conjunctions on {today}",
            Message=message
        )
        print(f"SNS alert fired — {critical_count} critical events")

    return {
        'statusCode': 200,
        'critical_count': critical_count,
        'caution_count':  caution_count,
        'safe_count':     safe_count,
        'alert_fired':    critical_count > 0
    }
