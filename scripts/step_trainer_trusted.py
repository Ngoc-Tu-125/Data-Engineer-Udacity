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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1723040888410 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1723040888410")

# Script generated for node Customer Trusted
CustomerTrusted_node1723040887977 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_curated", transformation_ctx="CustomerTrusted_node1723040887977")

# Script generated for node SQL Query
SqlQuery5715 = '''
SELECT s.*
FROM step_trainer_landing s
WHERE EXISTS (
    SELECT 1
    FROM customer_curated c
    WHERE c.serialnumber = s.serialnumber
)

'''
SQLQuery_node1723040890338 = sparkSqlQuery(glueContext, query = SqlQuery5715, mapping = {"step_trainer_landing":StepTrainerLanding_node1723040888410, "customer_curated":CustomerTrusted_node1723040887977}, transformation_ctx = "SQLQuery_node1723040890338")

# Script generated for node Amazon S3
AmazonS3_node1723040891530 = glueContext.getSink(path="s3://my-aws-datalake-bucket/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723040891530")
AmazonS3_node1723040891530.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="step_trainer_trusted")
AmazonS3_node1723040891530.setFormat("json")
AmazonS3_node1723040891530.writeFrame(SQLQuery_node1723040890338)
job.commit()