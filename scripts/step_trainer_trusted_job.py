import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1722966652638 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_curated", transformation_ctx="CustomerCurated_node1722966652638")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1722972147110 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1722972147110")

# Script generated for node Join
Join_node1722972159770 = Join.apply(frame1=StepTrainerLanding_node1722972147110, frame2=CustomerCurated_node1722966652638, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1722972159770")

# Script generated for node Amazon S3
AmazonS3_node1722972261172 = glueContext.getSink(path="s3://my-aws-datalake-bucket/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722972261172")
AmazonS3_node1722972261172.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="step_trainer_trusted")
AmazonS3_node1722972261172.setFormat("json")
AmazonS3_node1722972261172.writeFrame(Join_node1722972159770)
job.commit()