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
CustomerTrusted_node1723039918231 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1723039918231")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1723039918717 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1723039918717")

# Script generated for node Join
Join_node1723039953606 = Join.apply(frame1=CustomerTrusted_node1723039918231, frame2=AccelerometerTrusted_node1723039918717, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1723039953606")

# Script generated for node SQL Query
SqlQuery5390 = '''
SELECT DISTINCT
    serialNumber,
    customerName,
    email,
    phone,
    birthDay,
    registrationDate,
    lastUpdateDate,
    shareWithPublicAsOfDate,
    shareWithResearchAsOfDate,
    shareWithFriendsAsOfDate
FROM
    myDataSource
WHERE
    shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1723039954596 = sparkSqlQuery(glueContext, query = SqlQuery5390, mapping = {"myDataSource":Join_node1723039953606}, transformation_ctx = "SQLQuery_node1723039954596")

# Script generated for node Amazon S3
AmazonS3_node1723039956929 = glueContext.getSink(path="s3://my-aws-datalake-bucket/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723039956929")
AmazonS3_node1723039956929.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="customer_curated")
AmazonS3_node1723039956929.setFormat("json")
AmazonS3_node1723039956929.writeFrame(SQLQuery_node1723039954596)
job.commit()