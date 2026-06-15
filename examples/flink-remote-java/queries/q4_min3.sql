-- Nexmark Q4 — BISECTION SMOKE TEST (Stage 2b)
--
-- Adds the interval JOIN. The INSERT here is exactly q4_optimized's *inner*
-- subquery written straight to the sink — no outer aggregation, no SET hints.
--
-- Schema deltas vs q4_min2:
--   - auction: bring back `auction_id`, `start_ts`, `expire_ts`, watermark
--   - bid:     bring back `bid_auction_id`, `bid_ts`, watermark
--   - sink:    PRIMARY KEY on `auction_id` (one row per auction, upsert by id)
--
-- Question: does the interval JOIN + inner GROUP BY + upsert-kafka combo
-- submit and run?
--
-- If yes:
--   → JOIN + per-auction grouped agg + upsert-kafka all fine
--   → the breaker in q4_optimized is the OUTER aggregation (nested agg cascade)
--     or one of the SET hints
--   → Stage 2c: add the outer GROUP BY back (full q4_optimized minus SETs)
-- If no (silent shutdown like q4_optimized):
--   → the interval JOIN itself is the breaker
--   → next: try a regular equi-join without the time-interval predicate

CREATE TABLE auction (
    event ROW<
        `$type`      STRING,
        id           BIGINT,
        seller       BIGINT,
        category     STRING,
        name         STRING,
        initialPrice BIGINT,
        dateTime     BIGINT,
        expires      BIGINT
    >,
    `timestamp` BIGINT,
    auction_id AS event.id,
    category   AS event.category,
    start_ts   AS TO_TIMESTAMP_LTZ(event.dateTime, 3),
    expire_ts  AS TO_TIMESTAMP_LTZ(event.expires,  3),
    WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_min3_auctions',
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
    bid_ts         AS TO_TIMESTAMP_LTZ(event.dateTime, 3),
    WATERMARK FOR bid_ts AS bid_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_min3_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

-- Sink: one row per auction (PK = auction_id). The interval JOIN naturally
-- filters out non-auction / non-bid records (NULL fields fail BETWEEN), so
-- no explicit WHERE filter is needed here.
CREATE TABLE nexmark_q4 (
    auction_id  BIGINT,
    category    STRING,
    final_price BIGINT,
    `ts`        BIGINT,
    PRIMARY KEY (auction_id) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'output',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format'                   = 'json',
    'value.format'                 = 'json'
);

INSERT INTO nexmark_q4
SELECT
    A.auction_id,
    A.category,
    MAX(B.event.price)  AS final_price,
    MAX(A.`timestamp`)  AS `ts`
FROM auction A
JOIN bid B
    ON  A.auction_id = B.bid_auction_id
    AND B.bid_ts BETWEEN A.start_ts AND A.expire_ts
GROUP BY
    A.auction_id,
    A.category;

-- end of file
