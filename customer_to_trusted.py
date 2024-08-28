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
CustomerLanding_node1724832128232 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_landing", transformation_ctx="CustomerLanding_node1724832128232")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1724832194458 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1724832128232}, transformation_ctx = "SQLQuery_node1724832194458")

# Script generated for node Amazon S3
AmazonS3_node1724832336551 = glueContext.getSink(path="s3://jmbucketyroi/project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724832336551")
AmazonS3_node1724832336551.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
AmazonS3_node1724832336551.setFormat("json")
AmazonS3_node1724832336551.writeFrame(SQLQuery_node1724832194458)
job.commit()