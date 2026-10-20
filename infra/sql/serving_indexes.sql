-- Indexes for the two query patterns of serving.mart.obt_card_txn, with EXPLAIN ANALYZE before and after.
--   1. card investigation: one card's payments in the last 90 days, newest first      -> B-tree (card_id, txn_ts)
--   2. dashboard: spend per unit over the last 7 days                                 -> BRIN (txn_ts)
-- Each query runs twice; the second (warm cache) plan is the one to read.
-- Run (repo root):
--   docker compose -f infra/docker-compose.yaml exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d serving' < infra/sql/serving_indexes.sql

DROP INDEX IF EXISTS mart.ix_obt_card_ts;
DROP INDEX IF EXISTS mart.brin_obt_ts;
VACUUM ANALYZE mart.obt_card_txn;
SELECT card_id AS card FROM mart.obt_card_txn GROUP BY 1 ORDER BY count(*) DESC LIMIT 1 \gset
SELECT max(txn_ts) AS latest FROM mart.obt_card_txn \gset

\echo '== table'
SELECT count(*) AS rows, pg_size_pretty(pg_relation_size('mart.obt_card_txn')) AS table_size FROM mart.obt_card_txn;

\echo '== before: card lookup'
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_ts, merchant_name, amount_vnd, currency FROM mart.obt_card_txn
WHERE card_id = :'card' AND txn_ts >= :'latest'::timestamp - interval '90 days' ORDER BY txn_ts DESC LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_ts, merchant_name, amount_vnd, currency FROM mart.obt_card_txn
WHERE card_id = :'card' AND txn_ts >= :'latest'::timestamp - interval '90 days' ORDER BY txn_ts DESC LIMIT 50;
\echo '== before: 7-day spend by unit'
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_unit, sum(amount_vnd) FROM mart.obt_card_txn
WHERE txn_ts >= :'latest'::timestamp - interval '7 days' GROUP BY txn_unit;
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_unit, sum(amount_vnd) FROM mart.obt_card_txn
WHERE txn_ts >= :'latest'::timestamp - interval '7 days' GROUP BY txn_unit;

CREATE INDEX ix_obt_card_ts ON mart.obt_card_txn (card_id, txn_ts DESC);
CREATE INDEX brin_obt_ts ON mart.obt_card_txn USING brin (txn_ts);
CREATE INDEX btree_obt_ts ON mart.obt_card_txn (txn_ts);   -- only to compare sizes, dropped below
\echo '== index sizes'
SELECT relname AS index, pg_size_pretty(pg_relation_size(oid)) AS size FROM pg_class
WHERE relname IN ('ix_obt_card_ts', 'brin_obt_ts', 'btree_obt_ts') ORDER BY pg_relation_size(oid) DESC;
DROP INDEX mart.btree_obt_ts;
ANALYZE mart.obt_card_txn;

\echo '== after: card lookup'
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_ts, merchant_name, amount_vnd, currency FROM mart.obt_card_txn
WHERE card_id = :'card' AND txn_ts >= :'latest'::timestamp - interval '90 days' ORDER BY txn_ts DESC LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_ts, merchant_name, amount_vnd, currency FROM mart.obt_card_txn
WHERE card_id = :'card' AND txn_ts >= :'latest'::timestamp - interval '90 days' ORDER BY txn_ts DESC LIMIT 50;
\echo '== after: 7-day spend by unit'
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_unit, sum(amount_vnd) FROM mart.obt_card_txn
WHERE txn_ts >= :'latest'::timestamp - interval '7 days' GROUP BY txn_unit;
EXPLAIN (ANALYZE, BUFFERS) SELECT txn_unit, sum(amount_vnd) FROM mart.obt_card_txn
WHERE txn_ts >= :'latest'::timestamp - interval '7 days' GROUP BY txn_unit;
