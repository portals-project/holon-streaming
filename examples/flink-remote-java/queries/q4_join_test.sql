-- Nexmark Q4 — JOIN ISOLATION TEST
--
-- Purpose: determine whether the auction↔bid join still matches once the
-- `* 1000` timestamp scaling is removed (Beam emits getMillis() = ms already).
--
-- This is q4_stage_1's inner query ONLY (interval/regular join + per-auction
-- MAX bid), with:
--   * the `* 1000` removed (FIX 1),
--   * schema fixes (category BIGINT, itemName, initialBid),
--   * $type filters,
--   * NO outer GROUP BY (that is the part every single-job version chokes on).
--
-- It is sunk to `output` in the Q4 consumer shape (category / avg_win_price /
-- ts) so the existing OutputConsumerJson logs an [OUTPUT] line per emitted row.
--
-- Interpretation:
--   * [OUTPUT] lines appear  -> the join matches fine at ms scale; the breaker
--                               is the chained OUTER aggregation in one job.
--   * still zero output      -> removing `* 1000` broke join matching; the
--                               issue is the timestamp scale / 4s watermark.

CREATE TABLE auction (
    event ROW<
        `$type`    STRING,
        id         BIGINT,
        seller     BIGINT,
        category   BIGINT,
        itemName   STRING,
        initialBid BIGINT,
        dateTime   BIGINT,
        expires    BIGINT
    >,
    `timestamp` BIGINT,
    auction_id AS event.id,
    category   AS event.category,
    start_ts   AS TO_TIMESTAMP_LTZ(event.dateTime, 3),   -- no * 1000
    expire_ts  AS TO_TIMESTAMP_LTZ(event.expires,  3),   -- no * 1000
    WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_jointest_auctions',
    'scan.startup.mode'            = 'earliest-offset'
);

CREATE TABLE bid (
    event ROW<
        `$type`  STRING,
        auction  BIGINT,
        bidder   BIGINT,
        price    BIGINT,
        dateTime BIGINT,
        extra    STRING
    >,
    `timestamp` BIGINT,
    bid_auction_id AS event.auction,
    bid_ts         AS TO_TIMESTAMP_LTZ(event.dateTime, 3),   -- no * 1000
    WATERMARK FOR bid_ts AS bid_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_jointest_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

-- Same sink shape as q4 so the consumer's OutputRecordQ4 (category/avg/ts) parses.
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

-- Inner join + per-auction MAX only (no outer aggregation).
-- One emitted row per auction; here avg_win_price carries that auction's
-- winning (max) bid price purely so the row is visible via the consumer.
INSERT INTO nexmark_q4
SELECT
    CAST(A.category AS STRING)        AS category,
    CAST(MAX(B.event.price) AS DOUBLE) AS avg_win_price,
    MAX(A.`timestamp`)                AS `ts`
FROM auction A
JOIN bid B
    ON  A.auction_id = B.bid_auction_id
    AND B.bid_ts BETWEEN A.start_ts AND A.expire_ts   -- dynamic bound, like q4_stage_1
WHERE A.event.`$type` LIKE '%Auction%'
  AND B.event.`$type` LIKE '%Bid%'
GROUP BY
    A.auction_id,
    A.category;

-- end of file
