-- force Blink planner + streaming mode
SET 'table.planner' = 'blink';
SET 'execution.runtime-mode' = 'STREAMING';

--------------------------------------------------------------------------------
-- 1) SOURCE TABLES
--------------------------------------------------------------------------------
CREATE TABLE auction (
                         event ROW<
    `$type`      STRING,  -- "Auction"
                         id            BIGINT, -- auction ID
                         seller        BIGINT, -- seller ID
                         category      STRING, -- item category
                         name          STRING, -- item name
                         initialPrice  BIGINT, -- starting price in USD
                         dateTime      BIGINT, -- auction start (epoch seconds)
                         expires       BIGINT  -- auction end   (epoch seconds)
                             >,
                         `timestamp`        BIGINT,  -- Kafka log-append time

    -- flatten:
                         auction_id   AS event.id,
                         category     AS event.category,
                         start_ts     AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                         expire_ts    AS TO_TIMESTAMP_LTZ(event.expires   * 1000, 3),
                         WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
      'connector'                    = 'kafka',
      'topic'                        = 'input',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format'                       = 'json',
      'json.ignore-parse-errors'     = 'true',
      'properties.group.id'          = 'nexmark_q4_auctions',
      'scan.startup.mode'            = 'earliest-offset'
      );

CREATE TABLE bid (
                     event ROW<
    `$type`    STRING,  -- "Bid"
                     auction    BIGINT,  -- auction ID
                     bidder     BIGINT,  -- bidder ID
                     price      BIGINT,  -- bid price in USD
                     dateTime   BIGINT,  -- bid time (epoch seconds)
                     extra      STRING   -- any extra JSON
                         >,
                     `timestamp`        BIGINT,  -- Kafka log-append time

    -- flatten:
                     bid_auction_id AS event.auction,
                     bid_ts         AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                     WATERMARK FOR bid_ts AS bid_ts - INTERVAL '4' SECOND
) WITH (
      'connector'                    = 'kafka',
      'topic'                        = 'input',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format'                       = 'json',
      'json.ignore-parse-errors'     = 'true',
      'properties.group.id'          = 'nexmark_q4_bids',
      'scan.startup.mode'            = 'earliest-offset'
      );

--------------------------------------------------------------------------------
-- 2) UPSET-KAFKA SINK: compaction‐enabled “output” topic
--------------------------------------------------------------------------------
CREATE TABLE nexmark_q4 (
                            category      STRING,
                            avg_win_price DOUBLE,
                            `ts`   BIGINT,
                            PRIMARY KEY (category) NOT ENFORCED
) WITH (
      'connector'                    = 'upsert-kafka',
      'topic'                        = 'output',
      'properties.bootstrap.servers' = 'kafka:9092',
      'key.format'                   = 'json',
      'value.format'                 = 'json'
      );

--------------------------------------------------------------------------------
-- 3) INSERT: compute per‐auction MAX, then AVG per category, continuously
--------------------------------------------------------------------------------
INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final) AS avg_win_price,
    Q.`timestamp` AS `ts`
FROM (
         SELECT
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
             A.`timestamp`
     ) AS Q
GROUP BY
    Q.category,
    Q.`timestamp`;
