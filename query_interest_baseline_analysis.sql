SELECT
COUNT(DISTINCT query)
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests` i
WHERE _date >= '2023-01-09'
;--4,053,069

SELECT
COUNT(DISTINCT query)
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests_adhoc`
WHERE _date <= '2023-01-20'
;--161,369,291

SELECT
DISTINCT query 
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests_adhoc`
WHERE _date <= '2023-01-20' AND query LIKE "%&#%"
;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.query_interests` AS (
SELECT
  REPLACE(REPLACE(query, '&quot;', '"'),"&#39;","'") AS clean_query,
  *
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests_adhoc` i
WHERE _date <= '2023-01-20'
)
;--  &quot; -> "
 --  &#39; -> '

SELECT
DISTINCT clean_query 
FROM `etsy-data-warehouse-dev.nlao.query_interests`
WHERE clean_query LIKE "%&%"
;


-- BASIC STATS FOR QUERY BIN
SELECT 
  bin,
  COUNT(DISTINCT query),
  SUM(sessions),
  SUM(gms)/100 as gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics`
GROUP BY 1
;

-- QUERY COUNT/SESSION/GMS COVERAGE
-- QUERY BIN
SELECT 
  bin,
  COUNT(q.query) AS query_count,
  COUNT(CASE WHEN q.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN q.query ELSE NULL END) AS interest_query_count,
  SUM(q.sessions) AS query_session_count,
  SUM(CASE WHEN q.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN q.sessions ELSE NULL END) AS interest_query_sessions_count,
  SUM(gms)/100 AS query_gms,
  SUM(CASE WHEN q.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN gms ELSE NULL END)/100 AS interest_query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` q
GROUP BY 1
;

-- QUERY INTENT TABLE DEDUP
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.query_intent_labels_dedupped` AS (
  WITH cte AS (
  SELECT
    query_raw,
    inference.label,
    inference.confidence,
    RANK() OVER (PARTITION BY query_raw ORDER BY inference.confidence DESC) AS rk
  FROM `etsy-data-warehouse-prod.arizona.query_intent_labels`
  )
  SELECT
    DISTINCT query_raw,
    label,
    confidence 
  FROM cte
  WHERE rk = 1
);

-- QUERY INTENT
SELECT
  CASE WHEN confidence >= 0.5 THEN label ELSE 'Unsure' END AS label,
  COUNT(l.query) AS query_count,
  COUNT(CASE WHEN l.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN l.query ELSE NULL END) AS interest_query_count,
  SUM(l.sessions) AS query_session_count,
  SUM(CASE WHEN l.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN l.sessions ELSE NULL END) AS interest_query_sessions_count,
  SUM(l.gms)/100 AS query_gms,
  SUM(CASE WHEN l.query IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN l.gms ELSE NULL END)/100 AS interest_query_gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_intent_labels_dedupped` q 
  ON q.query_raw = l.query
GROUP BY 1
;

-- # OF INTEREST MAPPED TO QUERY
WITH cte AS (
SELECT
  query,
  COUNT(DISTINCT display_name) AS concept_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
-- WHERE score >= 0.1
GROUP BY 1
)
SELECT
  bin,
  CASE WHEN concept_count IS NULL THEN 0 ELSE concept_count END AS concept_count,
  COUNT(*) AS query_count,
  SUM(l.sessions) AS query_session_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN cte q 
  ON q.query = l.query
GROUP BY 1,2
;

-- # OF INTEREST TYPE MAPPED TO QUERY
WITH cte AS (
SELECT
  query,
  COUNT(DISTINCT attribute_type) AS concept_type_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
-- WHERE score >= 0.1
GROUP BY 1
)
SELECT
  bin,
  CASE WHEN concept_type_count IS NULL THEN 0 ELSE concept_type_count END AS concept_type_count,
  COUNT(*) AS query_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN cte q 
  ON q.query = l.query
GROUP BY 1,2
;

-- INTEREST TYPE
SELECT
  attribute_type,
  display_name,
  COUNT(DISTINCT query) AS query_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
WHERE score >= 0.1
GROUP BY 1,2
;

SELECT
  attribute_type,
  COUNT(DISTINCT query) AS query_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
WHERE score >= 0.1
GROUP BY 1
;

SELECT
  bin,
  CASE WHEN attribute_type IS NOT NULL THEN attribute_type ELSE 'null' END AS attribute_type,
  COUNT(DISTINCT l.query) AS query_count
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query = q.query
GROUP BY 1,2
;

SELECT
  bin,
  CASE WHEN attribute_type IS NOT NULL THEN attribute_type ELSE 'null' END AS attribute_type,
  CASE WHEN display_name IS NOT NULL THEN display_name ELSE 'null' END AS display_name,
  COUNT(DISTINCT l.query) AS query_count
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query = q.query
GROUP BY 1,2,3
;

