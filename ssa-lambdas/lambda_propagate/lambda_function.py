import json, boto3, time
from datetime import datetime, timezone
from decimal import Decimal

athena = boto3.client('athena', region_name='ap-south-1')
dynamo = boto3.resource('dynamodb', region_name='ap-south-1')
WORKGROUP = 'ssa-workgroup'

def run_query(sql):
    resp = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)
    qid = resp['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(2)
    if status != 'SUCCEEDED':
        raise RuntimeError(f"Query failed: {qid}")
    rows = athena.get_query_results(QueryExecutionId=qid)['ResultSet']['Rows']
    headers = [c['VarCharValue'] for c in rows[0]['Data']]
    return [{headers[i]: col.get('VarCharValue','') for i,col in enumerate(row['Data'])} for row in rows[1:]]

def approx_velocity(x, y, z):
    r = (x**2 + y**2 + z**2)**0.5
    v = 398600**0.5 / r**0.5
    return (-y/r * v, x/r * v, 0.0)

def propagate_72hr(x, y, z, vx, vy, vz):
    positions = []
    for h in range(0, 73, 3):
        px = x + vx * 3600 * h
        py = y + vy * 3600 * h
        pz = z + vz * 3600 * h
        positions.append({
            'hour': h,
            'x_km': round(px, 2),
            'y_km': round(py, 2),
            'z_km': round(pz, 2),
            'altitude_km': round((px**2 + py**2 + pz**2)**0.5 - 6371, 2)
        })
    return positions

def compute_avoidance(pos_a, pos_b):
    windows = []
    min_dist = float('inf')
    min_hour = 0
    for pa, pb in zip(pos_a, pos_b):
        dist = ((pa['x_km']-pb['x_km'])**2 + (pa['y_km']-pb['y_km'])**2 + (pa['z_km']-pb['z_km'])**2)**0.5
        windows.append({'hour': pa['hour'], 'distance_km': round(dist, 4)})
        if dist < min_dist:
            min_dist = dist
            min_hour = pa['hour']
    first_24 = [w for w in windows if w['hour'] <= 24]
    best = max(first_24, key=lambda w: w['distance_km'])
    return {
        'min_dist': round(min_dist, 4),
        'min_hour': min_hour,
        'best_hour': best['hour'],
        'best_dist': round(best['distance_km'], 4),
        'recommendation': (
            f"Optimal maneuver window at T+{best['hour']}hr "
            f"when separation is {best['distance_km']:.1f} km. "
            f"Closest approach predicted at T+{min_hour}hr ({min_dist:.4f} km). "
            f"Execute delta-V burn before T+{max(0, min_hour-6)}hr."
        )
    }

def lambda_handler(event, context):
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    traj_table = dynamo.Table('Trajectories72hr')
    rec_table  = dynamo.Table('Recommendations')

    critical_pairs = run_query(f"""
        SELECT target_sat, threat_sat, distance_km
        FROM ssa_gold.fact_collision_risks
        WHERE run_date = '{today}' AND risk_level = 'CRITICAL'
        ORDER BY distance_km ASC LIMIT 10
    """)

    results = []
    seen = set()

    for pair in critical_pairs:
        target, threat = pair['target_sat'], pair['threat_sat']
        key = f"{target}||{threat}"
        if key in seen:
            continue
        seen.add(key)

        sats = run_query(f"""
            SELECT satellite_name, x_km, y_km, z_km
            FROM ssa_silver.satellites_xyz
            WHERE run_date = '{today}'
              AND satellite_name IN ('{target}', '{threat}')
            LIMIT 2
        """)
        if len(sats) < 2:
            continue

        xa, ya, za = float(sats[0]['x_km']), float(sats[0]['y_km']), float(sats[0]['z_km'])
        xb, yb, zb = float(sats[1]['x_km']), float(sats[1]['y_km']), float(sats[1]['z_km'])

        pos_a = propagate_72hr(xa, ya, za, *approx_velocity(xa, ya, za))
        pos_b = propagate_72hr(xb, yb, zb, *approx_velocity(xb, yb, zb))
        av = compute_avoidance(pos_a, pos_b)

        # Store trajectory as JSON STRING — avoids all float/Decimal issues
        traj_table.put_item(Item={
            'satellite_pair': f"{target} || {threat}",
            'epoch_hour':     Decimal('0'),
            'run_date':       today,
            'trajectory_json': json.dumps(av),          # pure string, no floats
            'closest_approach_km': Decimal(str(av['min_dist'])),
            'maneuver_window_hour': str(av['best_hour']),
        })

        rec_table.put_item(Item={
            'run_date':       today,
            'target_sat':     target,
            'threat_sat':     threat,
            'recommendation': av['recommendation'],
            'maneuver_hour':  str(av['best_hour']),
            'closest_km':     Decimal(str(av['min_dist'])),
        })

        results.append(av['recommendation'])

    print(f"Processed {len(results)} critical pairs")
    return {'statusCode': 200, 'pairs_processed': len(results), 'recommendations': results[:3]}
