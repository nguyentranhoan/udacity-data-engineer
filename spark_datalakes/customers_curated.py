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
table_name = "customer_trusted"
output_path = "s3://hoannt19-udacity/customers_curated"

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
    table_name="accelerometer_landing",
    transformation_ctx="source_accelerometer",
    )
source_customer_df = source_customer.toDF()
emails = source_customer_df.select(col("user")).distinct()
email_list = [item['user'] for item in emails.collect()]

# get all data from data catalog for customer trust zone
source_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="source_data",
    )
# Convert from dynamic frame to data-frame
source_records_as_df = source_data.toDF()
verified_data = source_records_as_df.na.drop(subset=["shareWithResearchAsOfDate"])
curated_data = verified_data.filter(verified_data.email.isin(email_list))

# Write to output location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(curated_data, glueContext, "curated"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
    },
    transformation_ctx=f"curated-data-customer")

# Commit job
job.commit()