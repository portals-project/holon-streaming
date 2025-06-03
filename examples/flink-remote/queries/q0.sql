CREATE TABLE bid
(
    event       ROW< `$type` STRING,
    auction     BIGINT,
    bidder      BIGINT,
    price       BIGINT,
    `dateTime`  BIGINT,
    extra       STRING >,
    `timestamp` BIGINT,
    -- Flattened top-level fields for query access
    auction AS event.auction,
    bidder AS event.bidder,
    price AS event.price,
    `dateTime_ts` AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
    WATERMARK FOR `dateTime_ts` AS `dateTime_ts` - INTERVAL '4' SECOND
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
-- 2) SINK TABLE: write flat fields into Kafka topic "output"
--    (emit dateTime as TIMESTAMP(3) so that consumer sees a JSON string)
--------------------------------------------------------------------------------
CREATE TABLE nexmark_q0
    (
    auction    BIGINT,
    price      BIGINT,
    bidder     BIGINT,
    `dateTime` TIMESTAMP(3),
    `timestamp` BIGINT,
    extra      STRING
    )
    WITH (
        'connector' = 'kafka',
        'topic' = 'output',
        'properties.bootstrap.servers' = 'kafka:9092',
        'format' = 'json'
        );


--------------------------------------------------------------------------------
-- 3) PASS‐THROUGH INSERT: copy every record from bid → nexmark_q0
--    casting epoch seconds → TIMESTAMP(3) for dateTime.
--------------------------------------------------------------------------------
INSERT INTO nexmark_q0
SELECT B.auction,
       B.price,
       B.bidder,
       CAST(B.`dateTime_ts` AS TIMESTAMP(3)) AS `dateTime`,
       B.`timestamp`,
       B.event.extra
FROM bid B
