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

# Script generated for node Customer Curated
CustomerCurated_node1724840824556 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724840824556")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724840827555 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724840827555")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724840822645 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1724840822645")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.*, a.* from s
join a
on a.timestamp = s.sensorreadingtime
join c 
on c.serialnumber = s.serialnumber;
'''
SQLQuery_node1724840912356 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AccelerometerTrusted_node1724840827555, "s":StepTrainerTrusted_node1724840822645, "c":CustomerCurated_node1724840824556}, transformation_ctx = "SQLQuery_node1724840912356")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1724841017690 = glueContext.getSink(path="s3://jmbucketyroi/project/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1724841017690")
MachineLearningCurated_node1724841017690.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1724841017690.setFormat("json")
MachineLearningCurated_node1724841017690.writeFrame(SQLQuery_node1724840912356)
job.commit()