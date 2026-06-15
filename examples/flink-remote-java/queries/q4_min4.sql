-- Nexmark Q4 — BISECTION SMOKE TEST (Stage 2c)
--
-- Adds the outer aggregation back. This is now structurally identical to
-- q4_optimized — same nested-aggregation cascade, same upsert-kafka sink with
-- PK on `category` — but WITHOUT the 3 SET hints
-- (mini-batch / two-phase / state-ttl).
--
-- Question: does the nested aggregation cascade (inner JOIN+GROUP BY feeding
-- an outer GROUP BY) submit and run?
--
-- If yes:
--   → nested aggregation + upsert-kafka are fine on their own
--   → the breaker in q4_optimized is one of the 3 SET hints
--   → Stage 2d: add the SETs back one at a time
-- If no (silent shutdown):
--   → the nested-agg cascade itself is the breaker
--   → next: try splitting it into two jobs via an intermediate Kafka topic
--     (which is what the original two-stage q4.sql / q4_stage_1+2 did)

CREATE TABLE auction (
    event ROW<
        `$type`      STRING,
        id           BIGINT,
        seller       BIGINT,
        category     BIGINT,
        itemName     STRING,
        initialBid   BIGINT,
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
    'properties.group.id'          = 'nexmark_q4_min4_auctions',
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
    'properties.group.id'          = 'nexmark_q4_min4_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

-- Sink: one row per category (PK = category) — same as the real q4_optimized.
CREATE TABLE nexmark_q4 (
    category      BIGINT,
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
    Q.category,
    AVG(Q.final_price)  AS avg_win_price,
    MAX(Q.ts)           AS `ts`
FROM (
    SELECT
        A.auction_id,
        A.category,
        MAX(B.event.price)  AS final_price,
        MAX(A.`timestamp`)  AS ts
    FROM auction A
    JOIN bid B
        ON  A.auction_id = B.bid_auction_id
        AND B.bid_ts BETWEEN A.start_ts AND A.expire_ts
    -- Both tables read the same `input` topic; restrict each side to its own
    -- event type (mirrors the official Nexmark auction/bid event-type views).
    WHERE A.event.`$type` LIKE '%Auction%'
      AND B.event.`$type` LIKE '%Bid%'
    GROUP BY
        A.auction_id,
        A.category
) AS Q
GROUP BY
    Q.category;

-- end of file
