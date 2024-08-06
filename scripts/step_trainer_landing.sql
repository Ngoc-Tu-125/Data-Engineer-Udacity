CREATE EXTERNAL TABLE IF NOT EXISTS my_datalake_database.step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject FLOAT
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
LOCATION 's3://my-aws-datalake-bucket/step_trainer_landing/'
TBLPROPERTIES ('classification' = 'json');
