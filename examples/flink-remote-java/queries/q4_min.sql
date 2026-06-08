-- Nexmark Q4 — BISECTION SMOKE TEST (Stage 1)
--
-- Purpose: prove that the q4 pipeline can submit ANY job at all.
-- Strips everything from the real q4: no bid table, no JOIN, no nested
-- aggregation, no SET hints. Just auction → COUNT per category → upsert-kafka.
--
-- If this submits and reaches RUNNING:
--   → upsert-kafka + a plain grouped aggregation work fine
--   → the problem is the JOIN, nested aggregation, or one of the SET hints
--   → next step: add the bid table + interval JOIN back
-- If this ALSO fails to submit (same silent shutdown):
--   → suspect is upsert-kafka or the session/planner state itself
--   → next step: swap upsert-kafka for plain kafka + a TUMBLE window (like q7)

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
    'properties.group.id'          = 'nexmark_q4_min_auctions',
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

-- The `input` topic carries all three Nexmark event types (auction, bid, person)
-- mixed together. With json.ignore-parse-errors=true, non-auction records reach
-- this table with NULL fields. The IS NOT NULL filter drops them; in the real
-- q4_optimized the interval JOIN does this implicitly (NULL start_ts / expire_ts
-- fail the BETWEEN predicate).
INSERT INTO nexmark_q4
SELECT
    category,
    COUNT(*)         AS cnt,
    MAX(`timestamp`) AS `ts`
FROM auction
WHERE category IS NOT NULL
GROUP BY category;

-- end of file
