--------------------------------------------------------------------------------
-- 1) SOURCE TABLE: read raw Bid events from Kafka topic "input"
--------------------------------------------------------------------------------

CREATE TABLE bids_in (
                         event ROW<
    `$type`   STRING,  -- e.g. "Bid"
                         auction   BIGINT,  -- auction ID
                         bidder    BIGINT,  -- bidder ID
                         price     BIGINT,  -- bid price in USD
                         dateTime  BIGINT,  -- event timestamp (epoch seconds)
                         extra     STRING   -- any extra JSON field
                             >,
                         `timestamp` BIGINT,       -- Kafka log-append time (the broker timestamp)
    -- Flattened columns (not strictly needed for pass-through, but useful for time-based watermark):
                         auction AS event.auction,
                         bidder  AS event.bidder,
                         price   AS event.price,
                         dateTime_ts AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
                         WATERMARK FOR dateTime_ts AS dateTime_ts - INTERVAL '4' SECOND
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'input',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true',
        'properties.group.id' = 'nexmark_q0_consumer',
        'sink.partitioner' = 'round-robin',
        'scan.startup.mode' = 'earliest-offset'
        );


--------------------------------------------------------------------------------
-- 2) SINK TABLE: write the identical Bid JSONs into Kafka topic "q0-output"
--------------------------------------------------------------------------------

CREATE TABLE bids_out (
                          event ROW<
    `$type`   STRING,
                          auction   BIGINT,
                          bidder    BIGINT,
                          price     BIGINT,
                          dateTime  BIGINT,
                          extra     STRING
                              >,
                          `timestamp` BIGINT
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'output',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
        );


--------------------------------------------------------------------------------
-- 3) PASS-THROUGH INSERT: simply copy every record from bids_in → bids_out
--------------------------------------------------------------------------------

INSERT INTO bids_out
SELECT
    event,
    `timestamp`
FROM bids_in;