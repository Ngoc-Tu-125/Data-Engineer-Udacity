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
CustomerTrusted_node1722970825516 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722970825516")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722970862467 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1722970862467")

# Script generated for node Join
Join_node1722970893999 = Join.apply(frame1=CustomerTrusted_node1722970825516, frame2=AccelerometerLanding_node1722970862467, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1722970893999")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    myDataSource.serialNumber,
    myDataSource.customerName,
    myDataSource.email,
    myDataSource.phone,
    myDataSource.birthDay,
    myDataSource.registrationDate,
    myDataSource.lastUpdateDate,
    myDataSource.shareWithPublicAsOfDate,
    myDataSource.shareWithResearchAsOfDate,
    myDataSource.shareWithFriendsAsOfDate
FROM
    myDataSource
WHERE
    myDataSource.shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1722971133512 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1722970893999}, transformation_ctx = "SQLQuery_node1722971133512")

# Script generated for node Amazon S3
AmazonS3_node1722971034136 = glueContext.getSink(path="s3://my-aws-datalake-bucket/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722971034136")
AmazonS3_node1722971034136.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="customer_curated")
AmazonS3_node1722971034136.setFormat("json")
AmazonS3_node1722971034136.writeFrame(SQLQuery_node1722971133512)
job.commit()