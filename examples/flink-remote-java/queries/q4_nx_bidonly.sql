-- Q4 split — STAGE 1: bid view passthrough
-- Does the single source + bid view + sink emit at all? One [OUTPUT] per bid.
-- category column carries the bid's target auction id (just so we can see rows);
-- avg_win_price carries the bid price.
-- Expect: MANY [OUTPUT] lines (bids dominate the stream). If ~0 -> the source/view
-- or the $type filter is the problem, not the join.

CREATE TABLE nexmark (
    event ROW<
        `$type` STRING, id BIGINT, seller BIGINT, category BIGINT, itemName STRING,
        initialBid BIGINT, expires BIGINT, auction BIGINT, bidder BIGINT, price BIGINT,
        dateTime BIGINT, extra STRING
    >,
    `timestamp` BIGINT,
    rowtime AS TO_TIMESTAMP_LTZ(event.dateTime, 3),
    WATERMARK FOR rowtime AS rowtime - INTERVAL '4' SECOND
) WITH (
    'connector' = 'kafka', 'topic' = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json', 'json.ignore-parse-errors' = 'true',
    'properties.group.id' = 'nexmark_q4_nx_bidonly',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE nexmark_q4 (
    category STRING, avg_win_price DOUBLE, `ts` BIGINT,
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka', 'topic' = 'output',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json', 'value.format' = 'json'
);

CREATE VIEW bid AS
SELECT event.auction AS auction, event.price AS price, rowtime AS `dateTime`, `timestamp`
FROM nexmark WHERE event.`$type` LIKE '%Bid%';

INSERT INTO nexmark_q4
SELECT
    CAST(B.auction AS STRING) AS category,
    CAST(B.price AS DOUBLE)   AS avg_win_price,
    B.`timestamp`             AS `ts`
FROM bid B;

-- end of file
