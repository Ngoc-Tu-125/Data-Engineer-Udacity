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

# Script generated for node Customer Landing
CustomerLanding_node1723039028948 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_landing", transformation_ctx="CustomerLanding_node1723039028948")

# Script generated for node SQL Query
SqlQuery5565 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1723039033501 = sparkSqlQuery(glueContext, query = SqlQuery5565, mapping = {"myDataSource":CustomerLanding_node1723039028948}, transformation_ctx = "SQLQuery_node1723039033501")

# Script generated for node Amazon S3
AmazonS3_node1723039051944 = glueContext.getSink(path="s3://my-aws-datalake-bucket/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723039051944")
AmazonS3_node1723039051944.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="customer_trusted")
AmazonS3_node1723039051944.setFormat("json")
AmazonS3_node1723039051944.writeFrame(SQLQuery_node1723039033501)
job.commit()