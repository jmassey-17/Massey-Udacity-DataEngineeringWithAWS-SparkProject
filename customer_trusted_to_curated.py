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

# Script generated for node Step Data
StepData_node1724838800500 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="StepData_node1724838800500")

# Script generated for node Customers Trusted
CustomersTrusted_node1724838751103 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="CustomersTrusted_node1724838751103")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724838802730 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724838802730")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM (
    SELECT 
        c.*,
        ROW_NUMBER() OVER (PARTITION BY c.serialnumber ORDER BY c.serialnumber) AS row_num
    FROM c
    JOIN s ON c.serialnumber = s.serialnumber
    JOIN a ON a.timestamp = s.sensorreadingtime
    WHERE c.sharewithresearchasofdate IS NOT NULL
) AS subquery
WHERE row_num = 1;
'''
SQLQuery_node1724838789544 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":CustomersTrusted_node1724838751103, "s":StepData_node1724838800500, "a":AccelerometerTrusted_node1724838802730}, transformation_ctx = "SQLQuery_node1724838789544")

# Script generated for node Amazon S3
AmazonS3_node1724839376278 = glueContext.getSink(path="s3://jmbucketyroi/project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724839376278")
AmazonS3_node1724839376278.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
AmazonS3_node1724839376278.setFormat("json")
AmazonS3_node1724839376278.writeFrame(SQLQuery_node1724838789544)
job.commit()