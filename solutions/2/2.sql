CREATE TABLE payments
(
    id       String,
    date     Date,
    category String,
    purpose  String,
    money    Int,
    idx      Int
) ENGINE = ReplacingMergeTree(idx)
      ORDER BY (date, category, id);

CREATE MATERIALIZED VIEW payments_mv TO payments AS
SELECT id,
       date,
       category,
       anyLast(purpose) as purpose,
       anyLast(money)   as money,
       anyLast(idx)     as idx
FROM (
         SELECT JSONExtractString(value, 'id')           AS id,
                toDate(JSONExtractString(value, 'date')) AS date,
                JSONExtractString(value, 'category')     AS category,
                JSONExtractString(value, 'purpose')      AS purpose,
                JSONExtractInt(value, 'money')           AS money,
                JSONExtractInt(value, 'index')           AS idx
         FROM source
         WHERE JSONExtractString(value, 'type') = 'payment'
         )
GROUP BY id,
         date,
         category
ORDER BY date, category, id;

INSERT INTO payments
SELECT JSONExtractString(value, 'id')           AS id,
       toDate(JSONExtractString(value, 'date')) AS date,
       JSONExtractString(value, 'category')     AS category,
       JSONExtractString(value, 'purpose')      AS purpose,
       JSONExtractInt(value, 'money')           AS money,
       JSONExtractInt(value, 'index')           AS idx
FROM source
WHERE JSONExtractString(value, 'type') = 'payment'
