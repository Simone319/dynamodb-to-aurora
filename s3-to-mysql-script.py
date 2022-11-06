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

connection_mysql8_options_target_backuprestore = {
    "url": "jdbc:mysql://your.mysql.host.here:3306/YourDatabase",
    "dbtable": "your-MySQL-table",
    "user": "your-user-name",
    "password": "your-password",
    "customJdbcDriverS3Path": "s3://<path-to-connector>/mysql-connector-j-8.0.31.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"}

# Script generated for node Amazon S3
AmazonS3_node = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="glue-table",
    transformation_ctx="AmazonS3_node",
)

# Script generated for node Apply Mapping
ApplyMapping_node = ApplyMapping.apply(
    frame=AmazonS3_node,
    # change the mappings below to be your own data mapping logic
    mappings=[
        ("item.partitionKey.S", "string", "partitionKey", "int"),
        ("item.value.S", "string", "value", "string"),
    ],
    transformation_ctx="ApplyMapping_node",
)

MySQLtable_node = glueContext.write_from_options(
    frame_or_dfc=ApplyMapping_node,
    connection_type="mysql",
    connection_options=connection_mysql8_options_target_backuprestore)

job.commit()
