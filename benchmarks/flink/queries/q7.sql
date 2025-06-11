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
        'properties.group.id' = 'nexmark_q7_consumer',
        'sink.partitioner' = 'round-robin',
        'scan.startup.mode' = 'earliest-offset'
        );


CREATE TABLE nexmark_q7
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


INSERT INTO nexmark_q7
SELECT B.auction,
       B.price,
       B.bidder,
       CAST(B.`dateTime_ts` AS TIMESTAMP(3)) AS `dateTime`,
       B.`timestamp`,
       B.event.extra
FROM bid B
         JOIN (SELECT MAX(price) AS maxprice,
                      window_end AS `dateTime`
               FROM TABLE(
                       TUMBLE(TABLE bid, DESCRIPTOR(`dateTime_ts`), INTERVAL '27:46:40' HOUR TO SECOND)
                    )
               GROUP BY window_start, window_end) B1
              ON B.price = B1.maxprice
WHERE CAST(B.`dateTime_ts` AS TIMESTAMP(3))
          BETWEEN B1.`dateTime` - INTERVAL '27:46:40' HOUR TO SECOND AND B1.`dateTime`;


-- '27:46:40' is 100k seconds which is a window lenght of 10k
