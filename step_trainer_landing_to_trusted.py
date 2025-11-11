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

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1762899625907 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1762899625907")

# Script generated for node CustomerCurated
CustomerCurated_node1762899723172 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1762899723172")

# Script generated for node JoinStepTraining
JoinStepTraining_node1762899750378 = Join.apply(frame1=CustomerCurated_node1762899723172, frame2=StepTrainerLanding_node1762899625907, keys1=["serialnumber"], keys2=["serialNumber"], transformation_ctx="JoinStepTraining_node1762899750378")

# Script generated for node DropFieldSerialNumberFromOneSource
DropFieldSerialNumberFromOneSource_node1762900227496 = DropFields.apply(frame=JoinStepTraining_node1762899750378, paths=["serialnumber"], transformation_ctx="DropFieldSerialNumberFromOneSource_node1762900227496")

# Script generated for node SQL Query
SqlQuery479 = '''
select distinct sensorReadingTime,serialNumber,
distanceFromObject from myDataSource
'''
SQLQuery_node1762899798918 = sparkSqlQuery(glueContext, query = SqlQuery479, mapping = {"myDataSource":DropFieldSerialNumberFromOneSource_node1762900227496}, transformation_ctx = "SQLQuery_node1762899798918")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1762899891942 = glueContext.getSink(path="s3://stedi-human-analytics-data/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1762899891942")
StepTrainerTrusted_node1762899891942.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1762899891942.setFormat("json")
StepTrainerTrusted_node1762899891942.writeFrame(SQLQuery_node1762899798918)
job.commit()