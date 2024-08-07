import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1723039401730 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1723039401730")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1723039400745 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1723039400745")

# Script generated for node Join
Join_node1723039407227 = Join.apply(frame1=CustomerTrusted_node1723039401730, frame2=AccelerometerLanding_node1723039400745, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1723039407227")

# Script generated for node SQL Query
SqlQuery5382 = '''
SELECT
    user,
    timestamp,
    x,
    y,
    z
FROM
    myDataSource
WHERE
    shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1723039495290 = sparkSqlQuery(glueContext, query = SqlQuery5382, mapping = {"myDataSource":Join_node1723039407227}, transformation_ctx = "SQLQuery_node1723039495290")

# Script generated for node Amazon S3
AmazonS3_node1723039410197 = glueContext.getSink(path="s3://my-aws-datalake-bucket/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723039410197")
AmazonS3_node1723039410197.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="accelerometer_trusted")
AmazonS3_node1723039410197.setFormat("json")
AmazonS3_node1723039410197.writeFrame(SQLQuery_node1723039495290)
job.commit()