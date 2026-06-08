-- Nexmark Q4 — BISECTION SMOKE TEST (Stage 2a)
--
-- Same as q4_min, but adds the `bid` source table. The INSERT still reads
-- only from `auction` — the `bid` table is declared but unused.
--
-- Question: does just having a second CREATE TABLE on the same Kafka topic
-- (with the same json.ignore-parse-errors config) break submission?
--
-- If this submits and runs cleanly:
--   → two source tables on the same topic are fine
--   → advance to Stage 2b: add the interval JOIN
-- If this fails (silent shutdown like q4_optimized):
--   → declaring two tables on `input` is somehow the trigger
--   → next step: try giving each table a distinct consumer group / topic alias

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
    category AS event.category
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_min2_auctions',
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
    `timestamp` BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'input',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true',
    'properties.group.id'          = 'nexmark_q4_min2_bids',
    'scan.startup.mode'            = 'earliest-offset'
);

CREATE TABLE nexmark_q4 (
    category STRING,
    cnt      BIGINT,
    `ts`     BIGINT,
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'connector'                    = 'upsert-kafka',
    'topic'                        = 'output',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format'                   = 'json',
    'value.format'                 = 'json'
);

-- Same INSERT as q4_min — `bid` is declared but not referenced.
INSERT INTO nexmark_q4
SELECT
    category,
    COUNT(*)         AS cnt,
    MAX(`timestamp`) AS `ts`
FROM auction
WHERE category IS NOT NULL
GROUP BY category;

-- end of file
