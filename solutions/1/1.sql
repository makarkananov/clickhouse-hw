CREATE TABLE counters
(
    id      String,
    counter Int
) ENGINE = SummingMergeTree(counter)
      ORDER BY id;

CREATE MATERIALIZED VIEW counters_mv TO counters
AS
SELECT JSONExtractString(value, 'id')      as id,
       sum(JSONExtractInt(value, 'value')) as counter
FROM source
WHERE JSONExtractString(value, 'type') == 'counter'
GROUP BY JSONExtractString(value, 'id');

INSERT INTO counters
SELECT JSONExtractString(value, 'id')      as id,
       sum(JSONExtractInt(value, 'value')) as counter
FROM source
WHERE JSONExtractString(value, 'type') == 'counter'
GROUP BY JSONExtractString(value, 'id');