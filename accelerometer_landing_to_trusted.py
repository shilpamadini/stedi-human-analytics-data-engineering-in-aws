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

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1762894097106 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1762894097106")

# Script generated for node CustomerTrusted
CustomerTrusted_node1762894099828 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1762894099828")

# Script generated for node PrivacyJoin
PrivacyJoin_node1762894312070 = Join.apply(frame1=AccelerometerLanding_node1762894097106, frame2=CustomerTrusted_node1762894099828, keys1=["user"], keys2=["email"], transformation_ctx="PrivacyJoin_node1762894312070")

# Script generated for node SQL Query
SqlQuery580 = '''
select distinct timeStamp,user,x,y,z from myDataSource
'''
SQLQuery_node1762898218249 = sparkSqlQuery(glueContext, query = SqlQuery580, mapping = {"myDataSource":PrivacyJoin_node1762894312070}, transformation_ctx = "SQLQuery_node1762898218249")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1762894421767 = glueContext.getSink(path="s3://stedi-human-analytics-data/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1762894421767")
AccelerometerTrusted_node1762894421767.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1762894421767.setFormat("json")
AccelerometerTrusted_node1762894421767.writeFrame(SQLQuery_node1762898218249)
job.commit()