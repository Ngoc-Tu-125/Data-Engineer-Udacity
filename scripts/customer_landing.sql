CREATE DATABASE IF NOT EXISTS my_datalake_database;

CREATE EXTERNAL TABLE IF NOT EXISTS my_datalake_database.customer_landing (
    customerName STRING,
    email STRING,
    phone STRING,
    birthDay STRING,
    serialNumber STRING,
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://my-aws-datalake-bucket/customer_landing/'
TBLPROPERTIES ('classification' = 'json');
