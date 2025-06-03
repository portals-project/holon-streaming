-- Stage 1: JOIN auction and bid, compute MAX bid per auction

SET 'table.planner' = 'blink';
SET 'execution.runtime-mode' = 'STREAMING';

-- source tables (same as before)
CREATE TABLE auction (
                         event ROW<
        `$type` STRING,
                         id BIGINT,
                         seller BIGINT,
                         category STRING,
                         name STRING,
                         initialPrice BIGINT,
                         dateTime BIGINT,
                         expires BIGINT
                             >,
                         `timestamp` BIGINT,
                         auction_id AS event.id,
                         category AS event.category,
                         start_ts AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                         expire_ts AS TO_TIMESTAMP_LTZ(event.expires * 1000, 3),
                         WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'input',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true',
      'properties.group.id' = 'nexmark_q4_auctions',
      'scan.startup.mode' = 'earliest-offset'
      );

CREATE TABLE bid (
                     event ROW<
        `$type` STRING,
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     dateTime BIGINT,
                     extra STRING
                         >,
                     `timestamp` BIGINT,
                     bid_auction_id AS event.auction,
                     bid_ts AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                     WATERMARK FOR bid_ts AS bid_ts - INTERVAL '4' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'input',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true',
      'properties.group.id' = 'nexmark_q4_bids',
      'scan.startup.mode' = 'earliest-offset'
      );

-- intermediate output sink: one record per auction
CREATE TABLE per_auction_max (
                                 auction_id BIGINT,
                                 category   STRING,
                                 final      BIGINT,
                                 `timestamp` BIGINT,
                                 PRIMARY KEY (auction_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'auctionmax',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- insert the join + max result
INSERT INTO per_auction_max
SELECT
    A.auction_id,
    A.category,
    MAX(B.event.price) AS final,
    A.`timestamp`
FROM auction A
         JOIN bid B
              ON A.auction_id = B.bid_auction_id
                  AND B.bid_ts BETWEEN A.start_ts AND A.expire_ts
GROUP BY
    A.auction_id,
    A.category,
    A.`timestamp`;
