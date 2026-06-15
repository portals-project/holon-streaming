
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
-- '08:20:00' is 30k seconds which is a winodw lenght of 3k















































-- CREATE TABLE bid
-- (
--     event       ROW< `$type` STRING,
--     auction     BIGINT,
--     bidder      BIGINT,
--     price       BIGINT,
--     `dateTime`  BIGINT,
--     extra       STRING >,
--     `timestamp` BIGINT,
--     -- Flattened top-level fields for query access
--     auction AS event.auction,
--     bidder AS event.bidder,
--     price AS event.price,
--     `dateTime_ts` AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
--     WATERMARK FOR `dateTime_ts` AS `dateTime_ts` - INTERVAL '2' SECOND
-- )
--     WITH (
--         'connector' = 'kafka',
--         'topic' = 'input',
--         'properties.bootstrap.servers' = 'kafka:9092',
--         'format' = 'json',
--         'properties.group.id' = 'nexmark_q7_consumer',
--         'sink.partitioner' = 'round-robin',
--         'scan.startup.mode' = 'earliest-offset'
--         );
--
-- -- CREATE TABLE print_max (
-- --                            window_start TIMESTAMP(3),
-- --                            window_end   TIMESTAMP(3),
-- --                            maxprice     BIGINT
-- -- ) WITH ('connector'='print');
-- --
-- -- INSERT INTO print_max
-- -- SELECT
-- --     window_start,
-- --     window_end,
-- --     MAX(price) AS maxprice
-- -- FROM TABLE(
-- --         TUMBLE(
-- --             TABLE bid,
-- --             DESCRIPTOR(dateTime_ts),
-- --                 INTERVAL '27:46:40' HOUR TO SECOND
-- --         )
-- --      )
-- -- GROUP BY window_start, window_end;
--
--
-- CREATE TABLE nexmark_q7
-- (
--     auction    BIGINT,
--     price      BIGINT,
--     bidder     BIGINT,
--     `dateTime` TIMESTAMP(3),
--     `timestamp` BIGINT,
--     extra      STRING
-- )
--     WITH (
--         'connector' = 'kafka',
--         'topic' = 'output',
--         'properties.bootstrap.servers' = 'kafka:9092',
--         'format' = 'json'
--         );
--
-- -- 3) 10s tumbling + side‐input join, casting LTZ → TIMESTAMP for comparison
-- INSERT INTO nexmark_q7
-- SELECT
--     B.auction,
--     B.price,
--     B.bidder,
--     CAST(B.dateTime_ts AS TIMESTAMP(3))      AS `dateTime`,
--     B.`timestamp`                            AS `timestamp`,
--     B.event.extra                            AS extra
-- FROM bid AS B
--          JOIN (
--     SELECT
--         MAX(price)   AS maxprice,
--         window_end   AS `dateTime`
--     FROM TABLE(
--             TUMBLE(
--                 TABLE bid,
--                 DESCRIPTOR(dateTime_ts),
--                     INTERVAL '10' SECOND
--             )
--          )
--     GROUP BY window_start, window_end
-- ) AS W
--               ON B.price = W.maxprice
--                   AND CAST(B.dateTime_ts AS TIMESTAMP(3))
--                      BETWEEN W.`dateTime` - INTERVAL '10' SECOND
--                      AND W.`dateTime`;


-- '27:46:40' is 100k seconds which is a window lenght of 10k
-- '00:02:00' is 1 minute which is a window lenght of 120



-- CREATE TABLE bid
-- (
--     event       ROW< `$type` STRING,
--     auction     BIGINT,
--     bidder      BIGINT,
--     price       BIGINT,
--     `dateTime`  BIGINT,
--     extra       STRING >,
--     `timestamp` BIGINT,
--     -- Flattened top-level fields for query access
--     auction AS event.auction,
--     bidder AS event.bidder,
--     price AS event.price,
--     `dateTime_ts` AS TO_TIMESTAMP_LTZ(event.dateTime * 1000, 3),
--     WATERMARK FOR `dateTime_ts` AS `dateTime_ts` - INTERVAL '1' SECOND
-- )
--     WITH (
--         'connector' = 'kafka',
--         'topic' = 'input',
--         'properties.bootstrap.servers' = 'kafka:9092',
--         'format' = 'json',
--         'json.ignore-parse-errors' = 'true',
--         'properties.group.id' = 'nexmark_q7_consumer',
--         'sink.partitioner' = 'round-robin',
--         'scan.startup.mode' = 'earliest-offset'
--         );
--
--
-- CREATE TABLE nexmark_q7
-- (
--     auction    BIGINT,
--     price      BIGINT,
--     bidder     BIGINT,
--     `dateTime` TIMESTAMP(3),
--     `timestamp` BIGINT,
--     extra      STRING
-- )
--     WITH (
--         'connector' = 'kafka',
--         'topic' = 'output',
--         'properties.bootstrap.servers' = 'kafka:9092',
--         'format' = 'json'
--         );
--
--
-- INSERT INTO nexmark_q7
-- SELECT
--     1234567891011            AS auction,                 -- no auction in this agg
--     MAX(price)      AS price,                   -- our aggregated price
--     1234567891011            AS bidder,                  -- no bidder
--     window_end      AS dateTime,                -- use window_end as the timestamp
--     EXTRACT(EPOCH FROM window_end) * 1000       -- as a BIGINT “timestamp”
--                     AS `timestamp`,
--     ''              AS extra                    -- empty string for extra
-- FROM TABLE(
--         TUMBLE(
--             TABLE bid,
--             DESCRIPTOR(dateTime_ts),
--                 INTERVAL '00:01:00' HOUR TO SECOND
--         )
--      )
-- GROUP BY window_start, window_end;


-- INSERT INTO nexmark_q7
-- SELECT
--     B.auction,
--     B.price,
--     B.bidder,
--     CAST(B.dateTime_ts AS TIMESTAMP(3)) AS dateTime,
--     B.`timestamp`,
--     B.event.extra
-- FROM bid AS B
--          JOIN (
--     SELECT
--         window_start,
--         window_end,
--         MAX(price)    AS maxprice
--     FROM TABLE(
--             TUMBLE(
--                 TABLE bid,
--                 DESCRIPTOR(dateTime_ts),
--                     INTERVAL '00:01:00' HOUR TO SECOND
--             )
--          )
--     GROUP BY window_start, window_end
-- ) AS W   -- here W has _exactly_ these three columns
--               ON B.price = W.maxprice
--                   AND CAST(B.dateTime_ts AS TIMESTAMP(3)) >= W.window_start
--                   AND CAST(B.dateTime_ts AS TIMESTAMP(3)) <  W.window_end;


-- '27:46:40' is 100k seconds which is a window lenght of 10k
-- '00:50:00' is 3k minutes which is a window lenght of 3k
-- '00:01:00' is 1 minute which is a window lenght of 60
