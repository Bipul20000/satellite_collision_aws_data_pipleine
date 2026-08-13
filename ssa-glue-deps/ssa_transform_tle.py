# ssa_transform_tle.py — rewrite using sgp4 directly
import sys, json, math
from datetime import datetime, timezone

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import udf, col, lit

args     = getResolvedOptions(sys.argv, ['S3_BUCKET', 'RUN_DATE'])
BUCKET   = args['S3_BUCKET']
RUN_DATE = args['RUN_DATE']

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
logger      = glueContext.get_logger()

logger.info(f"SSA Transform starting — bucket={BUCKET}, date={RUN_DATE}")

# 1. Read bronze JSON
bronze_path = f"s3://{BUCKET}/bronze/satellites/date={RUN_DATE}/active_satellites_raw.json"
df_raw = spark.read.option("multiLine", "true").json(bronze_path)
logger.info(f"Schema: {df_raw.dtypes}")
logger.info(f"Count: {df_raw.count()}")

# 2. SGP4 UDF using sgp4 library directly (no skyfield)
pos_schema = StructType([
    StructField("X_km", DoubleType(), True),
    StructField("Y_km", DoubleType(), True),
    StructField("Z_km", DoubleType(), True),
])

def propagate(line1, line2):
    try:
        from sgp4.api import Satrec
        from sgp4.api import jday
        import math

        sat = Satrec.twoline2rv(line1, line2)
        now = datetime.now(timezone.utc)
        jd, fr = jday(now.year, now.month, now.day,
                      now.hour, now.minute, now.second + now.microsecond/1e6)
        e, r, v = sat.sgp4(jd, fr)

        if e != 0:
            return (None, None, None)

        x, y, z = float(r[0]), float(r[1]), float(r[2])
        if not (math.isfinite(x) and math.isfinite(y) and math.isfinite(z)):
            return (None, None, None)

        return (x, y, z)
    except Exception:
        return (None, None, None)

propagate_udf = udf(propagate, pos_schema)

# 3. Apply UDF
df_pos = df_raw.withColumn(
    "pos", propagate_udf(col("Raw_Line_1"), col("Raw_Line_2"))
).select(
    col("Satellite_Name"),
    col("Satellite_Number"),
    col("Classification"),
    col("Inclination").cast(DoubleType()),
    col("Eccentricity").cast(DoubleType()),
    col("Mean_Motion").cast(DoubleType()),
    col("pos.X_km").alias("X_km"),
    col("pos.Y_km").alias("Y_km"),
    col("pos.Z_km").alias("Z_km"),
    lit(RUN_DATE).alias("run_date"),
)

# 4. Split good vs quarantine
df_good = df_pos.filter(
    col("X_km").isNotNull() &
    col("Y_km").isNotNull() &
    col("Z_km").isNotNull()
)

df_bad = df_pos.filter(
    col("X_km").isNull() |
    col("Y_km").isNull() |
    col("Z_km").isNull()
).withColumn("quarantine_reason", lit("SGP4_error"))

good_count = df_good.count()
bad_count  = df_bad.count()
logger.info(f"Good: {good_count} | Quarantined: {bad_count}")

# 5. Write silver Parquet
silver_path     = f"s3://{BUCKET}/silver/satellites_xyz/"
quarantine_path = f"s3://{BUCKET}/silver/quarantine/satellites/"

df_good.write.mode("overwrite").partitionBy("run_date").parquet(silver_path)
df_bad.write.mode("overwrite").partitionBy("run_date").parquet(quarantine_path)

logger.info(f"Done. Written to {silver_path}")
