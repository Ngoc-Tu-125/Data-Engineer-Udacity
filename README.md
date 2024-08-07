# Data-Engineer-Udacity

### Step 1: Create S3 Directories
Create the following directories in your S3 bucket:
- customer_landing
- step_trainer_landing
- accelerometer_landing

Then copy the data files into these directories.

### Step 2: Create Glue Tables for Landing Zones
Base on the data declare in Project data page, create scripts to create Glue tables.

### Step 3: Query Tables using Athena
Run a query to inspect the data:
- SELECT * FROM my_datalake_database.customer_landing LIMIT 10;
- SELECT * FROM my_datalake_database.accelerometer_landing LIMIT 10;
- SELECT * FROM my_datalake_database.step_trainer_landing LIMIT 10;

The images of inspections in the images folder!

### Step 4: Create AWS Glue Jobs for Customer Trusted Zones
- First, Create IAM with the permissions: AmazonS3FullAccess and AWSGlueServiceRole
- Create Visual ETL and config with your IAM and your database.
- Data source -> SQL -> Data Target (S3 Bucket)
- Data source: Database: my_datalake_database, table: custumer_landing
- Data Target (S3 Bucket) - Choose option "Create a table in the Data Catalog and on subsequent runs, update the schema and add new partitions" -> Table name: custumer_trusted
- Download the script after run successfully


### Step 5: Create AWS Glue Jobs for Accelerometer Trusted Zones
- Create Visual ETL and config with your IAM and your database.
- Data source: Database: accelerometer_landing and custumer_trusted
- Join condition: accelerometer_landing.user == custumer_trusted.email
- Data Target (S3 Bucket) - accelerometer_trusted table
- Download the script after run successfully


### Step 6: Create AWS Glue Jobs for Customer Curated Zones
- Create Visual ETL and config with your IAM and your database.
- Data source: Database: accelerometer_trusted and custumer_trusted
- Join condition: accelerometer_trusted.user == custumer_trusted.email
- Data Target (S3 Bucket) - custumer_curated table
- Download the script after run successfully


### Step 7: Create AWS Glue Jobs for Step Trainer Trusted Zones
- Create Visual ETL and config with your IAM and your database.
- Data source: Database: step_trainer_landing and custumer_curated
- Join condition: step_trainer_landing.serialnumber = customers_curated.serialnumber
- Data Target (S3 Bucket) - step_trainer_trusted table
- Download the script after run successfully

### Step 7: Create AWS Glue Jobs for Machine Learning Curated
- Create Visual ETL and config with your IAM and your database.
- Data source: Database: step_trainer_trusted and accelerometer_trusted
- Join condition: step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timestamp
- Data Target (S3 Bucket) - machine_learning_curated table
- Download the script after run successfully
