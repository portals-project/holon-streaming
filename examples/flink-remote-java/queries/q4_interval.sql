-- Nexmark Q4 — SINGLE-JOB INTERVAL JOIN (performant target)
--
-- Same result as the official Q4 (average winning bid price per category) in ONE
-- Flink job, but using a TRUE interval join so Flink uses its bounded
-- interval-join operator (automatic state eviction) instead of the unbounded
-- regular join that q4_min4 / q4_optimized fall back to.
--
-- Why q4_min4 fell back to a regular join: its upper bound was `A.expire_ts` —
-- a second rowtime column, not a constant offset of one rowtime — so the planner
-- could not recognise an interval join. Here the upper bound is a CONSTANT
-- offset of start_ts, which is the shape Flink's interval join requires.
--
-- Tuning: AUCTION_MAX_DURATION (the INTERVAL below) must be >= the generator's
-- longest auction lifetime, or late winning bids are dropped. 60s is a generous
-- starting point for the scaled-down test config; measure MAX(expire_ts -
-- start_ts) and reduce it for tighter state. To restore exact Q4 semantics
-- (bids only up to the real expiry) add:  AND B.bid_ts <= A.expire_ts
--
-- All schema fixes from q4_min4 are kept: category BIGINT (Beam emits a number),
-- itemName / initialBid field names, $type filters, and no `* 1000`.

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
    start_ts   AS TO_TIMESTAMP_LTZ(event.dateTime, 3),
    expire_ts  AS TO_TIMESTAMP_LTZ(event.expires,  3),
    WATERMARK FOR start_ts AS start_ts - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_interval_auctions',
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
    'properties.group.id'          = 'nexmark_q4_interval_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

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

INSERT INTO nexmark_q4
SELECT
    CAST(Q.category AS STRING) AS category,
    AVG(Q.final_price)         AS avg_win_price,
    MAX(Q.ts)                  AS `ts`
FROM (
    SELECT
        A.auction_id,
        A.category,
        MAX(B.event.price)  AS final_price,
        MAX(A.`timestamp`)  AS ts
    FROM auction A
    JOIN bid B
        ON  A.auction_id = B.bid_auction_id
        -- CONSTANT upper bound -> qualifies as a Flink interval join (bounded state)
        AND B.bid_ts BETWEEN A.start_ts AND A.start_ts + INTERVAL '60' SECOND
    WHERE A.event.`$type` LIKE '%Auction%'
      AND B.event.`$type` LIKE '%Bid%'
    GROUP BY
        A.auction_id,
        A.category
) AS Q
GROUP BY
    Q.category;

-- end of file
