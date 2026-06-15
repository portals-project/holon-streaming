-- Define input table from Kafka
CREATE TABLE input_stream
(
    id    STRING,
    `value` INT
) WITH (
      'connector' = 'kafka',
      'topic' = 'input',
      'properties.bootstrap.servers' = 'kafka:9092',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
      );

-- Define output table to Kafka
CREATE TABLE output_stream
(
    id    STRING,
    `value` INT
) WITH (
      'connector' = 'kafka',
      'topic' = 'output',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json'
      );

-- Sample processing query
INSERT INTO output_stream
SELECT id, `value`
FROM input_stream
WHERE `value` > 10;
