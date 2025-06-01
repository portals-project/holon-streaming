--------------------------------------------------------------------------------
-- 1) SOURCE TABLE: read JSON‐auction events from Kafka topic "auctions"
--------------------------------------------------------------------------------

CREATE TABLE auctions_in (
                             event ROW<
    `$type`       STRING,  -- e.g. "Auction"
                             auction       BIGINT,  -- auction ID
                             seller        BIGINT,  -- seller ID
                             category      STRING,  -- item category
                             name          STRING,  -- item name
                             initialPrice  BIGINT,  -- starting price in USD
                             dateTime      BIGINT,  -- auction start time (epoch seconds)
                             expiration    BIGINT   -- auction end time (epoch seconds)
                                 >,
                             `timestamp` BIGINT,       -- Kafka log-append time
    -- Flattened columns for time semantics and joins:
                             auction_id AS event.auction,
                             category AS event.category,
                             dateTime_ts AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                             expiry_ts   AS TO_TIMESTAMP_LTZ(event.expiration * 1000, 3),
                             WATERMARK FOR dateTime_ts AS dateTime_ts - INTERVAL '4' SECOND
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'auctions',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'properties.group.id' = 'nexmark_q4_auctions',
        'scan.startup.mode' = 'earliest-offset'
        );


--------------------------------------------------------------------------------
-- 2) SOURCE TABLE: read JSON‐bid events from Kafka topic "input"
--------------------------------------------------------------------------------

CREATE TABLE bids_in (
                         event ROW<
    `$type`   STRING,  -- e.g. "Bid"
                         auction   BIGINT,  -- auction ID
                         bidder    BIGINT,  -- bidder ID
                         price     BIGINT,  -- bid price in USD
                         dateTime  BIGINT,  -- bid time (epoch seconds)
                         extra     STRING
                             >,
                         `timestamp` BIGINT,
                         auction_id  AS event.auction,
                         dateTime_ts AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                         WATERMARK FOR dateTime_ts AS dateTime_ts - INTERVAL '4' SECOND
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'input',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'properties.group.id' = 'nexmark_q4_bids',
        'scan.startup.mode' = 'earliest-offset'
        );


--------------------------------------------------------------------------------
-- 3) VIEW: compute, for each auction, the highest (winning) price so far
--------------------------------------------------------------------------------

CREATE VIEW winning_bids AS
SELECT
    auction_id,
    MAX(price) AS winning_price
FROM bids_in
GROUP BY auction_id;


--------------------------------------------------------------------------------
-- 4) SINK TABLE: write “average winning price per category” into "q4-output"
--------------------------------------------------------------------------------

CREATE TABLE avg_winning_price_category (
                                            category      STRING,
                                            avg_price_usd DOUBLE
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'output',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
        );


--------------------------------------------------------------------------------
-- 5) INSERT: join “auctions_in” ↔ “winning_bids”, then GROUP BY category
--------------------------------------------------------------------------------

INSERT INTO avg_winning_price_category
SELECT
    a.category,
    AVG(wb.winning_price) AS avg_price_usd
FROM auctions_in AS a
         JOIN winning_bids AS wb
              ON a.auction_id = wb.auction_id
GROUP BY a.category;
