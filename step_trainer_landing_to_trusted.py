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

# Script generated for node Step trainer landing
Steptrainerlanding_node1724839951352 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="Steptrainerlanding_node1724839951352")

# Script generated for node Customer Curated
CustomerCurated_node1724839999415 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724839999415")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.* from s 
join c 
on s.serialnumber = c.serialnumber;
'''
SQLQuery_node1724840072418 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":CustomerCurated_node1724839999415, "s":Steptrainerlanding_node1724839951352}, transformation_ctx = "SQLQuery_node1724840072418")

# Script generated for node Amazon S3
AmazonS3_node1724840199077 = glueContext.getSink(path="s3://jmbucketyroi/project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724840199077")
AmazonS3_node1724840199077.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
AmazonS3_node1724840199077.setFormat("json")
AmazonS3_node1724840199077.writeFrame(SQLQuery_node1724840072418)
job.commit()