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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1762901374018 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1762901374018")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1762901330595 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics-data/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1762901330595")

# Script generated for node JoinStepTrainerAccelerometer
JoinStepTrainerAccelerometer_node1762901479775 = Join.apply(frame1=AccelerometerTrusted_node1762901374018, frame2=StepTrainerTrusted_node1762901330595, keys1=["timeStamp"], keys2=["sensorReadingTime"], transformation_ctx="JoinStepTrainerAccelerometer_node1762901479775")

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1762901645052 = glueContext.getSink(path="s3://stedi-human-analytics-data/machinelearning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1762901645052")
MachineLearningCurated_node1762901645052.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1762901645052.setFormat("json")
MachineLearningCurated_node1762901645052.writeFrame(JoinStepTrainerAccelerometer_node1762901479775)
job.commit()