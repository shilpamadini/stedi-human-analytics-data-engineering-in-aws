# STEDI Human Balance Analytics
Data Engineering on AWS with Glue, Spark, and Athena

This project builds a data lakehouse solution for the STEDI Step Trainer — a device that helps users improve balance using real-time motion sensor data. The project's goal is to design a scalable data pipeline that processes raw sensor and customer data into a curated dataset ready for machine learning.
The curated output will help data scientists train models that predict human steps and balance patterns accurately.

## Project Summary

The project demonstrates how to use AWS Glue, S3, and Spark (PySpark) to build a multi-layered data pipeline that moves data through these zones:

Landing Zone: Raw JSON data from multiple sources (customer, step trainer, accelerometer)

Trusted Zone: Sanitized and filtered data (only records from consenting users)

Curated Zone: Aggregated and enriched datasets ready for ML modeling

Automates the ETL process using AWS Glue jobs and maintain data catalogs for all stages using AWS Glue Data Catalog and Athena.


## Execution Steps

1. Setup S3 Data Lake

Create an S3 bucket: stedi-human-analytics-data

Inside the bucket, create folders:

customer/landing/
accelerometer/landing/
step_trainer/landing/
customer/trusted/
accelerometer/trusted/
step_trainer/trusted/
customer/curated/
machinelearning/curated


Upload all JSON data files into the appropriate landing folders on S3.

2. Create AWS Glue Database

Open the AWS Glue Console → Databases → Create a database named stedi_db.

This will store metadata for all tables across landing, trusted, and curated zones.

3. the landing tables were manually created in Athena by executing SQL DDL scripts.

Steps:

Open the Athena Console, choose (or create) the database stedi_db.

For each dataset (customer, accelerometer, step_trainer), run the corresponding .sql script from the repo:

customer_landing.sql

accelerometer_landing.sql

step_trainer_landing.sql

4. Validate Landing Data in Athena

Open Athena, choose database stedi_db, and run simple queries:

```
SELECT COUNT(*) FROM stedi_db.customer_landing;
SELECT COUNT(*) FROM stedi_db.accelerometer_landing;
SELECT COUNT(*) FROM stedi_db.step_trainer_landing;
```

Expected row counts:

Table	Expected Rows
customer_landing	956
accelerometer_landing	81,273
step_trainer_landing	28,680


5. Create and Run Glue ETL Jobs (Trusted Zone)

customer_trusted job:
Filter customer data → only include records where sharewithresearchasofdate is not null.
Output → s3://stedi-human-analytics-data/customer/trusted/

accelerometer_trusted job:
Join accelerometer and customer data → keep only consenting users.
Output → s3://stedi-human-analytics-data/accelerometer/trusted/

Run both jobs and verify that new data appears in the S3 trusted folders.

Jobs are setup such way that they will automatically create an glue catalog tables
for 

Verify that Glue tables customer_trusted and accelerometer_trusted appear via Athena.


Open Athena, choose database stedi_db, and run simple queries:

```
SELECT COUNT(*) FROM stedi_db.customer_trusted;
SELECT COUNT(*) FROM stedi_db.accelerometer_trustd;
```
Expected counts:

Table	Expected Rows
customer_trusted	482
accelerometer_trusted	40,981

6. Create and Run Glue Jobs (Curated Zone)

customers_curated:
Join customer_trusted and accelerometer_trusted to include only customers with accelerometer data.
Output → customer/curated/

step_trainer_trusted:
Join step_trainer_landing with customers_curated on serial number.
Output → step_trainer/trusted/

machine_learning_curated:
Join step_trainer_trusted with accelerometer_trusted on timestamp to prepare ML training data.
Output → machine_learning/curated/

Jobs are setup such way that they will automatically create an glue catalog tables
for 

Verify that Glue tables customer_trusted and accelerometer_trusted appear via Athena.


Open Athena, choose database stedi_db, and run simple queries:

```
SELECT COUNT(*) FROM stedi_db.customer_curated;
SELECT COUNT(*) FROM stedi_db.step_trainer_trustd;
SELECT COUNT(*) FROM stedi_db.machine_learning_curated;
```
