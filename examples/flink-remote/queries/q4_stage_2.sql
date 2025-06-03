-- Stage 2: Read from per_auction_max and compute running average

SET 'table.planner' = 'blink';
SET 'execution.runtime-mode' = 'STREAMING';

-- input: from stage 1
CREATE TABLE per_auction_max (
                                 category STRING,
                                 final BIGINT,
                                 `timestamp` BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'auctionmax',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true',
      'properties.group.id' = 'nexmark_q4_stage2',
      'scan.startup.mode' = 'earliest-offset'
      );

-- final sink (same as before)
CREATE TABLE nexmark_q4 (
                            category STRING,
                            avg_win_price DOUBLE,
                            ts BIGINT,
                            PRIMARY KEY (category) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'output',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- running average per category
INSERT INTO nexmark_q4
SELECT
    category,
    AVG(final) AS avg_win_price,
    MAX(`timestamp`) AS ts
FROM per_auction_max
GROUP BY category;
