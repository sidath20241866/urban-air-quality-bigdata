import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw data
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="air_quality_db",
    table_name="raw"
)

# Convert to DataFrame
df = raw_dyf.toDF()

# Explode 'results' array
df_flat = df.withColumn("result", explode(col("results")))

# Select relevant nested fields
df_final = df_flat.select(
    col("partition_0").alias("city"),
    col("partition_1").alias("batch_time"),
    col("result.value").cast("double").alias("value"),
    col("result.parameter.name").alias("parameter"),
    col("result.period.datetimefrom.utc").alias("timestamp")
    # col("result.location")  <-- removed this
)

# Convert back to DynamicFrame
flattened_dyf = DynamicFrame.fromDF(df_final, glueContext, "flattened_dyf")

# Write to S3 in Parquet format, partitioned
glueContext.write_dynamic_frame.from_options(
    frame=flattened_dyf,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://urban-air-quality-pipeline/processed/",
        "partitionKeys": ["city", "batch_time"]
    },
    format_options={"compression": "snappy"}
)

job.commit()
