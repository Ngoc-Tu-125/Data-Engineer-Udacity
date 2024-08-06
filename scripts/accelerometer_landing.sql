CREATE EXTERNAL TABLE IF NOT EXISTS my_datalake_database.accelerometer_landing (
    user STRING,
    timestamp BIGINT,
    x FLOAT,
    y FLOAT,
    z FLOAT
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
LOCATION 's3://my-aws-datalake-bucket/accelerometer_landing/'
TBLPROPERTIES ('classification' = 'json');
