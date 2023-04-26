# trainer_trusted_to_curated.py
import sys
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# env variables
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
app_name = "accelerometer_sanitizer"
database_name = "udacity"
table_name = "accelerometer_trusted "
output_path = "s3://hoannt19-udacity/machine_learning_curated"

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


# get all user who appear for customers who have agreed to share their data,
source_customer = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="source_customer",
    )
source_customer_df = source_customer.toDF()
timeStamps = source_customer_df.select(col("timeStamp")).distinct()
timeStamp_list = [item['timeStamp'] for item in timeStamps.collect()]


# get all data from data catalog for step_trainer_landing table
source_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name='step_trainer_landing',
    transformation_ctx="step_trainer_landing",
    )
# Convert from dynamic frame to data-frame
source_records_as_df = source_data.toDF()
machine_learning_curated = source_records_as_df.filter(source_records_as_df.sensorReadingTime.isin(timeStamp_list))

# Write to output location, then create Glue Table called machine_learning_curated
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(machine_learning_curated, glueContext, "trainer-ML"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
    },
    transformation_ctx=f"step-ML")

# Commit job
job.commit()