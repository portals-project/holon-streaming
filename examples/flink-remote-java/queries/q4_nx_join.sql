-- Q4 split — STAGE 3: the JOIN only (no aggregation)
-- The linchpin test. One [OUTPUT] per matched (auction, bid) pair.
-- avg_win_price carries the matched bid price.
-- Interpretation:
--   MANY [OUTPUT]  -> the join works; the breaker is the aggregation cascade
--                     downstream (add the inner GROUP BY next, then the outer).
--   ~1 [OUTPUT]    -> the join itself barely matches; next: COUNT(*) probes and
--                     the watermark (idle partitions / time bound at ms scale).
--   0 / exception  -> the time attribute through the view breaks the time-bounded
--                     join planning.

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
    'properties.group.id' = 'nexmark_q4_nx_join',
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

CREATE VIEW bid AS
SELECT event.auction AS auction, event.price AS price, rowtime AS `dateTime`, `timestamp`
FROM nexmark WHERE event.`$type` LIKE '%Bid%';

INSERT INTO nexmark_q4
SELECT
    A.category               AS category,
    CAST(B.price AS DOUBLE)  AS avg_win_price,
    A.`timestamp`            AS `ts`
FROM auction A
JOIN bid B
    ON  A.id = B.auction
    AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires;

-- end of file
