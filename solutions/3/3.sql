CREATE TABLE payments_for_parents
(
    id       String,
    date     Date,
    category String,
    purpose  String,
    money    Int,
    idx      Int
) ENGINE = ReplacingMergeTree(idx)
      ORDER BY (date, category, id);

CREATE MATERIALIZED VIEW payments_for_parents_mv TO payments_for_parents AS
SELECT id,
       date,
       category,
       anyLast(purpose) as purpose,
       anyLast(money)   as money,
       anyLast(idx)     as idx
FROM (
         SELECT id,
                date,
                category,
                purpose,
                money,
                idx
         FROM payments
         WHERE category != 'gaming'
           AND category != 'useless'
         )
GROUP BY id,
         date,
         category
ORDER BY date, category, id;

INSERT INTO payments_for_parents
SELECT id,
       date,
       category,
       purpose,
       money,
       idx
FROM payments
WHERE category != 'gaming'
  AND category != 'useless'
