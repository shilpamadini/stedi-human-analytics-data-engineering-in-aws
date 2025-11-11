import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node CustomerLanding
CustomerLanding_node1762891851035 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1762891851035")

# Script generated for node PrivacyFilter
PrivacyFilter_node1762891922776 = Filter.apply(frame=CustomerLanding_node1762891851035, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="PrivacyFilter_node1762891922776")

# Script generated for node CustomerTrusted
CustomerTrusted_node1762891961632 = glueContext.getSink(path="s3://stedi-human-analytics-data/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1762891961632")
CustomerTrusted_node1762891961632.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
CustomerTrusted_node1762891961632.setFormat("json")
CustomerTrusted_node1762891961632.writeFrame(PrivacyFilter_node1762891922776)
job.commit()