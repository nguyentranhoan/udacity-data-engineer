import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://hoannt19-udacity/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680780299601 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1680780299601",
)

# Script generated for node Join
Join_node1680780323800 = Join.apply(
    frame1=AWSGlueDataCatalog_node1680780299601,
    frame2=S3bucket_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1680780323800",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1680780323800,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://hoannt19-udacity/accelerometer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
