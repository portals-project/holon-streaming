-- Q4 split — STAGE 2: auction view passthrough
-- Do auctions flow through the view, and is category non-null? One [OUTPUT] per auction.
-- avg_win_price is set to 0 (placeholder) so auction rows are visually distinct from bids.
-- Expect: MANY [OUTPUT] lines with category in 10..14 and avg=0.0. If ~0 -> auctions are
-- NOT reaching the query (the auction $type filter, or auction parsing) -> the join can
-- never match, which would explain everything.

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
    'properties.group.id' = 'nexmark_q4_nx_auctiononly',
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

CREATE VIEW auction AS
SELECT event.id AS id, CAST(event.category AS STRING) AS category,
       rowtime AS `dateTime`, TO_TIMESTAMP_LTZ(event.expires, 3) AS expires, `timestamp`
FROM nexmark WHERE event.`$type` LIKE '%Auction%';

INSERT INTO nexmark_q4
SELECT
    A.category          AS category,
    CAST(0 AS DOUBLE)   AS avg_win_price,
    A.`timestamp`       AS `ts`
FROM auction A;

-- end of file
