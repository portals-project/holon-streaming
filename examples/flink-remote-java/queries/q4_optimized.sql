-- Nexmark Q4 – Average Winning Bid Price per Category (Optimized, Single Job)
--
-- This query computes:
--   1) The winning (max) bid price for each closed auction
--   2) The running average of winning bid prices per category
--
-- Key fixes over the previous q4.sql / q4_stage_1 + q4_stage_2:
--
--   FIX 1 – Timestamp unit bug:
--     event.dateTime is produced by Beam's Instant.getMillis() and is already
--     in epoch MILLISECONDS. The old query multiplied by 1000, inflating all
--     timestamps by 1000×. This caused auction durations to appear ~1000×
--     longer, making the interval join retain state for thousands of seconds
--     instead of seconds, and slowing watermark advancement dramatically.
--
--   FIX 2 – GROUP BY `timestamp` removed:
--     The old outer GROUP BY included A.`timestamp` (the Beam event-time of
--     the auction record). Because every auction record has a unique event
--     time, this created one group per record – the AVG never actually
--     aggregated anything, and state grew without bound.
--
--   FIX 3 – Single Flink job:
--     Eliminates the intermediate Kafka topic ("auctionmax") and the extra
--     serialization/deserialization round-trip. Flink can optimise the whole
--     pipeline as one job graph with operator chaining.
--
--   FIX 4 – Flink SQL performance hints:
--     Mini-batch mode, two-phase aggregation, state TTL, and faster watermark
--     emission are enabled to reduce per-record overhead and shuffling.

-- mini-batch: buffer records for up to 500 ms or 5 000 records before flushing
-- through the aggregation operators – dramatically reduces per-record overhead
SET 'table.exec.mini-batch.enabled'       = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500ms';
SET 'table.exec.mini-batch.size'          = '5000';

-- two-phase (local + global) aggregation to cut down cross-network shuffling
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

-- state TTL: clean up state entries after 5 min of inactivity, prevents
-- unbounded state growth for completed auctions
SET 'table.exec.state.ttl' = '300s';

-- 1) SOURCE: Auctions
CREATE TABLE auction (
    event ROW<
        `$type`      STRING,
        id           BIGINT,
        seller       BIGINT,
        category     STRING,
        name         STRING,
        initialPrice BIGINT,
        dateTime     BIGINT,   -- auction start (epoch MILLISECONDS)
        expires      BIGINT    -- auction end   (epoch MILLISECONDS)
    >,
    `timestamp`  BIGINT,       -- Beam event-time (epoch ms), used for latency

    -- computed / flattened columns
    auction_id  AS event.id,
    category    AS event.category,
    start_ts    AS TO_TIMESTAMP_LTZ(event.dateTime, 3),   -- FIX 1: no * 1000
    expire_ts   AS TO_TIMESTAMP_LTZ(event.expires,  3),   -- FIX 1: no * 1000
    WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_opt_auctions',
    'scan.startup.mode'            = 'earliest-offset'
);

-- 2) SOURCE: Bids
CREATE TABLE bid (
    event ROW<
        `$type`  STRING,
        auction  BIGINT,
        bidder   BIGINT,
        price    BIGINT,
        dateTime BIGINT,   -- bid time (epoch MILLISECONDS)
        extra    STRING
    >,
    `timestamp`    BIGINT,

    bid_auction_id AS event.auction,
    bid_ts         AS TO_TIMESTAMP_LTZ(event.dateTime, 3),  -- FIX 1: no * 1000
    WATERMARK FOR bid_ts AS bid_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_opt_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

-- 3) SINK: upsert-kafka (one row per category, continuously updated)
CREATE TABLE nexmark_q4 (
    category      STRING,
    avg_win_price DOUBLE,
    `ts`          BIGINT,
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'output',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format'                   = 'json',
    'value.format'                 = 'json'
);

-- 4) QUERY
--    Inner: interval join auctions ↔ bids, MAX(price) per auction
--    Outer: AVG of winning prices per category
--
--    The `ts` column propagates the latest Beam event-time so the output
--    consumer can assign each result to the correct measurement window.
INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final_price)  AS avg_win_price,
    MAX(Q.ts)           AS `ts`
FROM (
    SELECT
        A.auction_id,
        A.category,
        MAX(B.event.price)  AS final_price,
        MAX(A.`timestamp`)  AS ts              -- latest Beam event-time for this auction
    FROM auction A
    JOIN bid B
        ON  A.auction_id = B.bid_auction_id
        AND B.bid_ts BETWEEN A.start_ts AND A.expire_ts
    GROUP BY
        A.auction_id,
        A.category                             -- FIX 2: no A.`timestamp` here
) AS Q
GROUP BY
    Q.category;                                -- FIX 2: no Q.`timestamp` here

-- end of file
