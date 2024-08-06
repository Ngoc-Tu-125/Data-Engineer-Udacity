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
CustomerTrusted_node1722969258164 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722969258164")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722966142903 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1722966142903")

# Script generated for node Join
Join_node1722969598919 = Join.apply(frame1=AccelerometerLanding_node1722966142903, frame2=CustomerTrusted_node1722969258164, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1722969598919")

# Script generated for node SQL Query
SqlQuery4880 = '''
SELECT
    myDataSource.user,
    myDataSource.x,
    myDataSource.y,
    myDataSource.z
FROM
    myDataSource
WHERE
    myDataSource.shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1722974714567 = sparkSqlQuery(glueContext, query = SqlQuery4880, mapping = {"myDataSource":Join_node1722969598919}, transformation_ctx = "SQLQuery_node1722974714567")

# Script generated for node Amazon S3
AmazonS3_node1722969686983 = glueContext.getSink(path="s3://my-aws-datalake-bucket/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722969686983")
AmazonS3_node1722969686983.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="accelerometer_trusted")
AmazonS3_node1722969686983.setFormat("json")
AmazonS3_node1722969686983.writeFrame(SQLQuery_node1722974714567)
job.commit()