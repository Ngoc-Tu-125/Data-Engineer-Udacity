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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1723041355842 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1723041355842")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1723041356484 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1723041356484")

# Script generated for node SQL Query
SqlQuery5299 = '''
SELECT
    step_trainer_trusted.sensorreadingtime AS timestamp,
    step_trainer_trusted.serialnumber,
    step_trainer_trusted.distanceFromObject,
    accelerometer_trusted.user,
    accelerometer_trusted.x,
    accelerometer_trusted.y,
    accelerometer_trusted.z
FROM
    step_trainer_trusted
INNER JOIN
    accelerometer_trusted
ON
    step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
'''
SQLQuery_node1723041438316 = sparkSqlQuery(glueContext, query = SqlQuery5299, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1723041355842, "accelerometer_trusted":AccelerometerTrusted_node1723041356484}, transformation_ctx = "SQLQuery_node1723041438316")

# Script generated for node Amazon S3
AmazonS3_node1723041472912 = glueContext.getSink(path="s3://my-aws-datalake-bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723041472912")
AmazonS3_node1723041472912.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="machine_learning_curated")
AmazonS3_node1723041472912.setFormat("json")
AmazonS3_node1723041472912.writeFrame(SQLQuery_node1723041438316)
job.commit()