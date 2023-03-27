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

# env variables
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
app_name = "customer_sanitizer"
database_name = "udacity"
table_name = "customer_landing"
output_path = "s3://hoannt19-udacity/customer_trusted"

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

source_data = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="source_data",
    )
# Convert from dynamic frame to data-frame
source_records_as_df = source_data.toDF()
sanitize_data = source_records_as_df.na.drop(subset=["sharewithresearchasofdate"])

# Write to output location
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(sanitize_data, glueContext, "sanitize"),
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": output_path,
    },
    transformation_ctx=f"customer-sanitize")

# Commit job
job.commit()