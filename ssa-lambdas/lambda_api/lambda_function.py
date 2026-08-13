import json, boto3, time
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
from decimal import Decimal

athena = boto3.client('athena', region_name='ap-south-1')
dynamo = boto3.resource('dynamodb', region_name='ap-south-1')
WORKGROUP = 'ssa-workgroup'


# ✅ FIX: robust Decimal encoder (global solution)
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


def run_query(sql):
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
    )
    qid = resp['QueryExecutionId']
    while True:
        status = athena.get_query_execution(
            QueryExecutionId=qid
        )['QueryExecution']['Status']['State']
        if status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(2)
    if status != 'SUCCEEDED':
        raise RuntimeError(f"Query failed: {qid}")
    rows = athena.get_query_results(
        QueryExecutionId=qid
    )['ResultSet']['Rows']
    headers = [c['VarCharValue'] for c in rows[0]['Data']]
    return [{headers[i]: col.get('VarCharValue','') for i,col in enumerate(row['Data'])} for row in rows[1:]]


def get_latest_date():
    rows = run_query("""
        SELECT MAX(run_date) as latest
        FROM ssa_gold.fact_collision_risks
    """)
    return rows[0]['latest'] if rows else datetime.now(timezone.utc).strftime('%Y-%m-%d')

def lambda_handler(event, context):
    print("Event:", json.dumps(event))
    route = event.get("routeKey", "")
    today = get_latest_date()

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
    }

    try:
        if route == "GET /summary":
            data = run_query(f"SELECT * FROM ssa_gold.risk_summary WHERE run_date = '{today}'")

        elif route == "GET /conjunctions":
            data = run_query(f"""
                SELECT target_sat, threat_sat, distance_km, risk_level
                FROM ssa_gold.fact_collision_risks
                WHERE run_date = '{today}'
                ORDER BY distance_km ASC LIMIT 20
            """)

        elif route == "GET /density":
            data = run_query(f"""
                SELECT altitude_band, satellite_count
                FROM ssa_gold.orbital_density
                WHERE run_date = '{today}'
                ORDER BY satellite_count DESC
            """)

        elif route == "GET /distribution":
            data = run_query(f"""
                SELECT distance_bucket, pair_count, risk_level
                FROM ssa_gold.distance_distribution
                WHERE run_date = '{today}'
                ORDER BY pair_count DESC
            """)

        elif route == "GET /dangerous":
            data = run_query(f"""
                SELECT satellite, conjunction_count, closest_approach_km, critical_events
                FROM ssa_gold.most_dangerous_satellites
                WHERE run_date = '{today}'
                ORDER BY critical_events DESC LIMIT 10
            """)

        elif route == "GET /trends":
            data = run_query("""
                SELECT run_date, conjunction_count, critical_count, caution_count, safe_count, closest_km
                FROM ssa_gold.conjunction_trends
                ORDER BY run_date DESC LIMIT 30
            """)

        elif route == "GET /recommendations":
            table = dynamo.Table('Recommendations')

            resp = table.query(
                KeyConditionExpression=Key('run_date').eq(today)
            )

            data = resp.get('Items', [])

            # sort by closest distance
            data.sort(key=lambda x: float(x.get('closest_km', 999)))

            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(data[:10], cls=DecimalEncoder)  # ✅ FIX HERE
            }

        else:
            return {
                'statusCode': 404,
                'headers': headers,
                'body': json.dumps({'error': f'Route not found: {route}'})
            }

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(data, cls=DecimalEncoder)  # ✅ FIX HERE ALSO
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': str(e)})
        }
