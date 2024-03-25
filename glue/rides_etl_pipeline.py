import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, date_format

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ride_input_cols = [
    "vendorid",
    "payment_type",
    "ratecodeid",
    "pulocationid",
    "dolocationid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "airport_fee",
    "congestion_surcharge",
    "total_amount",
    "improvement_surcharge",
    "tolls_amount",
    "tip_amount",
    "mta_tax",
    "extra",
    "fare_amount",
    "trip_distance",
    "passenger_count"
]

ride_out_cols = [
    "vendor_id",
    "pickup_datetime_id",
    "dropoff_datetime_id",
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "payment_type_id",
    "airport_fee",
    "congestion_surcharge",
    "total_amount",
    "improvement_surcharge",
    "tolls_amount",
    "tip_amount",
    "mta_tax",
    "extra",
    "fare_amount",
    "trip_distance",
    "passenger_count",
]

rides_preaction_query = """
    CREATE TABLE IF NOT EXISTS public.rides_fact (
        vendor_id INTEGER,
        pickup_datetime_id INTEGER,
        dropoff_datetime_id INTEGER,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        rate_code_id INTEGER,
        payment_type_id INTEGER,
        airport_fee REAL,
        congestion_surcharge REAL,
        total_amount REAL,
        improvement_surcharge REAL,
        tolls_amount REAL, 
        tip_amount REAL,
        mta_tax REAL,
        extra REAL,
        fare_amount REAL,
        trip_distance REAL,
        passenger_count INTEGER,
        FOREIGN KEY (pickup_datetime_id) REFERENCES datetime_dim (datetime_id),
        FOREIGN KEY (dropoff_datetime_id) REFERENCES datetime_dim (datetime_id),
        FOREIGN KEY (pickup_location_id) REFERENCES location_dim (location_id),
        FOREIGN KEY (dropoff_location_id) REFERENCES location_dim (location_id),
        FOREIGN KEY (rate_code_id) REFERENCES rate_code_dim (rate_code_id),
        FOREIGN KEY (payment_type_id) REFERENCES payment_type_dim (payment_type_id)
    );"""

datetime_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_datetime"
)
print("Schema for the locations DynamicFrame: \n", datetime_dyf.printSchema())

rides_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="fact_rides",
    transformation_ctx="rides_dyf"
)
print("Schema for the rides DynamicFrame: \n", rides_dyf.printSchema())

payment_type_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_payment_type"
)
print("Schema for the Payment Type DynamicFrame: \n", payment_type_dyf.printSchema())

rate_code_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="yellow-taxi",
    table_name="dim_rate_code"
)
print("Schema for the Rate Code DynamicFrame: \n", rate_code_dyf.printSchema())

datetime_spark_df = datetime_dyf.toDF()
rides_spark_df = rides_dyf.toDF()
payment_spark_df = payment_type_dyf.toDF()
rate_code_spark_df = rate_code_dyf.toDF()

rides_spark_df = rides_spark_df.withColumn(
    "tpep_pickup_datetime", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:00:00"))
rides_spark_df = rides_spark_df.withColumn(
    "tpep_dropoff_datetime", date_format(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:00:00"))
    
# Alias the DataFrames
rides_df = rides_spark_df.alias("rides")
datetime_df = datetime_spark_df.alias("datetime")

# Join the DataFrames and specify qualified column names
rides_datetime_join_df = rides_df.select(ride_input_cols).join(
    datetime_df.select(["datetime_id", "datetime"]), 
    rides_df["tpep_pickup_datetime"] == datetime_df["datetime"]
)
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("datetime_id", "pickup_datetime_id")
rides_datetime_join_df = rides_datetime_join_df.drop("datetime")

rides_datetime_join_df = rides_datetime_join_df.join(
    datetime_df.select(["datetime_id", "datetime"]), 
    rides_datetime_join_df["tpep_dropoff_datetime"] == datetime_df["datetime"]
)
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("datetime_id", "dropoff_datetime_id")
rides_datetime_join_df = rides_datetime_join_df.drop("datetime")

# cast rate_code and rename it
rides_datetime_join_df = rides_datetime_join_df.fillna(value=1.0, subset=['ratecodeid'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value=0.0, subset=['airport_fee'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value=0.0, subset=['congestion_surcharge'])
rides_datetime_join_df = rides_datetime_join_df.fillna(value=0.0, subset=['passenger_count'])

rides_datetime_join_df = (
    rides_datetime_join_df.withColumnRenamed("ratecodeid", "rate_code_id")
    .withColumn("rate_code_id", col("rate_code_id").cast("int")))
rides_datetime_join_df = rides_datetime_join_df.filter(col('rate_code_id') != 99)

rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("payment_type", "payment_type_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("vendorid", "vendor_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("pulocationid", "pickup_location_id")
rides_datetime_join_df = rides_datetime_join_df.withColumnRenamed("dolocationid", "dropoff_location_id")

rides_datetime_join_df.show(5)

# Show the result
final_spark_df = rides_datetime_join_df.select(ride_out_cols)
final_spark_df.show(5)

final_dyf = DynamicFrame.fromDF(final_spark_df, glueContext, "final_dyf")
final_dyf.printSchema()

final_dyf_mapped = final_dyf.apply_mapping(
    [
        ("vendor_id", "long", "vendor_id", "integer"),
        ("pickup_datetime_id", "long", "pickup_datetime_id", "integer"),
        ("dropoff_datetime_id", "long", "dropoff_datetime_id", "integer"),
        ("pickup_location_id", "long", "pickup_location_id", "integer"),
        ("dropoff_location_id", "long", "dropoff_location_id", "integer"),
        ("rate_code_id", "int", "rate_code_id", "integer"),
        ("payment_type_id", "long", "payment_type_id", "integer"),
        ("airport_fee", "double", "airport_fee", "float"),
        ("congestion_surcharge", "double", "congestion_surcharge", "float"),
        ("total_amount", "double", "total_amount", "float"),
        ("improvement_surcharge", "double", "improvement_surcharge", "float"),
        ("tolls_amount", "double", "tolls_amount", "float"),
        ("tip_amount", "double", "tip_amount", "float"),
        ("mta_tax", "double", "mta_tax", "float"),
        ("extra", "double", "extra", "float"),
        ("fare_amount", "double", "fare_amount", "float"),
        ("trip_distance", "double", "trip_distance", "float"),
        ("passenger_count", "double", "passenger_count", "integer"),
    ],
    transformation_ctx="final_dyf_mapped",
)
final_dyf_mapped.printSchema()

# Script generated for node Amazon Redshift
final_rides_dyf = glueContext.write_dynamic_frame.from_options(
    frame=final_dyf_mapped, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-471112663332-us-east-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "public.rides_fact", 
        "connectionName": "Redshift connection", 
        "preactions": rides_preaction_query,
    }, 
    transformation_ctx="final_rides_dyf"
)

job.commit()