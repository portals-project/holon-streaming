-- Nexmark Q4 — OFFICIAL STRUCTURE on Kafka (single source + views)
--
-- This mirrors the official nexmark-flink setup as closely as our wire format
-- allows, instead of our previous two-separate-tables design:
--
--   Official (ddl_kafka.sql):           Ours here:
--   ----------------------------        ----------------------------------
--   ONE `kafka` source table            ONE `nexmark` source table over `input`
--   event_type INT + 3 nested rows      our single `event` ROW tagged by `$type`
--   ONE computed `dateTime` rowtime      ONE computed `rowtime`
--   ONE WATERMARK                        ONE WATERMARK
--   auction/bid/person are VIEWS         auction/bid are VIEWS (person unused)
--   q4 joins the views                   q4 joins the views (verbatim structure)
--
-- The key change vs q4_min4/q4_interval: auction and bid now derive from a SINGLE
-- source operator and a SINGLE watermark (like the official), rather than two
-- independent Kafka tables with two independent watermarks. That is the official's
-- correctness-critical structure for the time-bounded join.
--
-- Only deviations from official q4: (1) our JSON shape (`event.$type`, dateTime as
-- BIGINT ms), (2) the sink is upsert-kafka in our consumer's OutputRecordQ4 shape
-- (category STRING / avg_win_price DOUBLE / ts BIGINT), (3) we carry ts = the Beam
-- event-time so the output consumer can window the results.

-- 1) SINGLE SOURCE over the `input` topic. `event` holds the UNION of the fields
--    we need from auctions and bids; non-matching fields arrive NULL.
CREATE TABLE nexmark (
    event ROW<
        `$type`    STRING,
        -- auction fields
        id         BIGINT,
        seller     BIGINT,
        category   BIGINT,
        itemName   STRING,
        initialBid BIGINT,
        expires    BIGINT,
        -- bid fields
        auction    BIGINT,
        bidder     BIGINT,
        price      BIGINT,
        -- shared
        dateTime   BIGINT,
        extra      STRING
    >,
    `timestamp` BIGINT,
    rowtime AS TO_TIMESTAMP_LTZ(event.dateTime, 3),
    WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_nexmark',
    'scan.startup.mode'            = 'earliest-offset'
);

-- 2) SINK in the consumer's Q4 shape (OutputRecordQ4: category/avg_win_price/ts).
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

-- 3) VIEWS over the single source (official structure). `rowtime` is preserved as
--    a time attribute through the alias `dateTime`, so the join below is the same
--    time-bounded join as the official q4. category is cast to STRING here so it
--    flows as STRING all the way to the sink PK / consumer.
CREATE VIEW auction AS
SELECT
    event.id                            AS id,
    CAST(event.category AS STRING)      AS category,
    rowtime                             AS `dateTime`,
    TO_TIMESTAMP_LTZ(event.expires, 3)  AS expires,
    `timestamp`                         AS `timestamp`
FROM nexmark
WHERE event.`$type` LIKE '%Auction%';

CREATE VIEW bid AS
SELECT
    event.auction  AS auction,
    event.price    AS price,
    rowtime        AS `dateTime`,
    `timestamp`    AS `timestamp`
FROM nexmark
WHERE event.`$type` LIKE '%Bid%';

-- 4) Official Q4 over the views:
--    inner = winning (MAX) bid per auction within its [dateTime, expires] window
--    outer = average winning price per category
INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final)        AS avg_win_price,
    MAX(Q.ts)           AS `ts`
FROM (
    SELECT
        A.id,
        A.category,
        MAX(B.price)        AS final,
        MAX(A.`timestamp`)  AS ts
    FROM auction A
    JOIN bid B
        ON  A.id = B.auction
        AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    GROUP BY
        A.id,
        A.category
) AS Q
GROUP BY
    Q.category;

-- end of file
