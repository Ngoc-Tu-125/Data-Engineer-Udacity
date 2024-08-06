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
CustomerLanding_node1722965416268 = glueContext.create_dynamic_frame.from_catalog(database="my_datalake_database", table_name="customer_landing", transformation_ctx="CustomerLanding_node1722965416268")

# Script generated for node SQL Query
SqlQuery4845 = '''
select * from myDataSource
where sharewithresearchasofdate is not null

'''
SQLQuery_node1722965446427 = sparkSqlQuery(glueContext, query = SqlQuery4845, mapping = {"myDataSource":CustomerLanding_node1722965416268}, transformation_ctx = "SQLQuery_node1722965446427")

# Script generated for node Customer Trusted
CustomerTrusted_node1722965463430 = glueContext.getSink(path="s3://my-aws-datalake-bucket/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1722965463430")
CustomerTrusted_node1722965463430.setCatalogInfo(catalogDatabase="my_datalake_database",catalogTableName="customer_trusted")
CustomerTrusted_node1722965463430.setFormat("json")
CustomerTrusted_node1722965463430.writeFrame(SQLQuery_node1722965446427)
job.commit()