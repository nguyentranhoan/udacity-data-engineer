import sys
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col


# env variables
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
app_name = "accelerometer_sanitizer"
database_name = "udacity"
table_name = "customers_curated"
output_path = "s3://hoannt19-udacity/step_trainer_trusted"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
conf = SparkConf()
conf.set("spark.driver.maxResultSize", "0")
conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
conf.set("spark.sql.debug.maxToStringFields", "1000")
pyspark = SparkSession.builder.appName(
    f"{app_name}").config(conf=conf).getOrCreate()


# get all user emails who appear in accelerometer_landing
source_customer = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="source_customer",
    )
source_customer_df = source_customer.toDF()
serial_numbers = source_customer_df.select(col("serialNumber")).distinct()
serial_number_list = [item['serialNumber'] for item in serial_numbers.collect()]

# get all data from data catalog for customer trust zone
source_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name='step_trainer_landing',
    transformation_ctx="step_trainer_landing",
    )
# Convert from dynamic frame to data-frame
source_records_as_df = source_data.toDF()
curated_data = source_records_as_df.filter(source_records_as_df.serialNumber.isin(serial_number_list))

# Write to output location, then create Glue Table called step_trainer_trusted
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(curated_data, glueContext, "trainer"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
    },
    transformation_ctx=f"step-trainer")

# Commit job
job.commit()