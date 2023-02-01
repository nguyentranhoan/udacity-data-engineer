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
table_name = "accelerometer_landing"
output_path = "s3://hoannt19-udacity/accelerometer_trusted"

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


# get all user email who agreed for research purpose
source_customer = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="customer_landing",
    transformation_ctx="source_customer",
    )
source_customer_df = source_customer.toDF()
research_purpose_agreed = source_customer_df.na.drop(subset=["shareWithResearchAsOfDate"])
emails = research_purpose_agreed.select(col("email")).distinct()
email_list = [item['email'] for item in emails.collect()]

# get all data from data catalog for accelerometer_landing
source_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="source_data",
    )
# Convert from dynamic frame to data-frame
source_records_as_df = source_data.toDF()
sanitize_data = source_records_as_df.filter(source_records_as_df.user.isin(email_list))

# Write to output location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(sanitize_data, glueContext, "sanitize"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
    },
    transformation_ctx=f"accelerometer-sanitize")

# Commit job
job.commit()