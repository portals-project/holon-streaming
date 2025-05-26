-- Register Kafka source table
CREATE TABLE input_table
(
    user_id STRING,
    action  STRING,
    ts      TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'input',
      'properties.bootstrap.servers' = 'kafka:9093',
      'format' = 'json',
      'scan.startup.mode' = 'earliest-offset'
      );

-- Register Kafka sink table
CREATE TABLE output_table
(
    user_id      STRING,
    action_count BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'output',
      'properties.bootstrap.servers' = 'kafka:9093',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Define transformation
INSERT INTO output_table
SELECT user_id, COUNT(*) AS action_count
FROM input_table
GROUP BY user_id;
