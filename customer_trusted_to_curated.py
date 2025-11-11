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

# Script generated for node CustomeTrusted
CustomeTrusted_node1762896593288 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/customer/trusted/"], "recurse": True}, transformation_ctx="CustomeTrusted_node1762896593288")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1762898754670 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1762898754670")

# Script generated for node JoinCurated
JoinCurated_node1762896674654 = Join.apply(frame1=CustomeTrusted_node1762896593288, frame2=AccelerometerTrusted_node1762898754670, keys1=["email"], keys2=["user"], transformation_ctx="JoinCurated_node1762896674654")

# Script generated for node SQLQueryCurated
SqlQuery384 = '''
select distinct serialnumber,
sharewithpublicasofdate,
birthday,registrationdate,
sharewithresearchasofdate,customername,
sharewithfriendsasofdate,email,
lastupdatedate,
phone
from myDataSource

'''
SQLQueryCurated_node1762898877850 = sparkSqlQuery(glueContext, query = SqlQuery384, mapping = {"myDataSource":JoinCurated_node1762896674654}, transformation_ctx = "SQLQueryCurated_node1762898877850")

# Script generated for node CustomerCurated
CustomerCurated_node1762896916591 = glueContext.getSink(path="s3://stedi-human-analytics-data/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1762896916591")
CustomerCurated_node1762896916591.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
CustomerCurated_node1762896916591.setFormat("json")
CustomerCurated_node1762896916591.writeFrame(SQLQueryCurated_node1762898877850)
job.commit()