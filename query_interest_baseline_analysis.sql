SELECT
COUNT(DISTINCT query)
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests` i
WHERE _date >= '2023-01-09'
;--4053069

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.query_interests` AS (
SELECT
  REPLACE(REPLACE(query, '&quot;', '"'),"&#39;","'") AS clean_query,
  *
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests` i
WHERE _date <= '2023-01-10'
)
;--  &quot; -> "
 --  &#39; -> '

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
WITH interest_query AS (
SELECT
  clean_query
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` q
JOIN `etsy-data-warehouse-dev.nlao.query_interests` i
  ON q.query = i.clean_query
)
SELECT 
  bin,
  COUNT(q.query) AS query_count,
  COUNT(CASE WHEN q.query IN (SELECT clean_query FROM interest_query) THEN q.query ELSE NULL END) AS interest_query_count,
  SUM(q.sessions) AS query_session_count,
  SUM(CASE WHEN q.query IN (SELECT clean_query FROM interest_query) THEN q.sessions ELSE NULL END) AS interest_query_sessions_count,
  SUM(gms)/100 AS query_gms,
  SUM(CASE WHEN q.query IN (SELECT clean_query FROM interest_query) THEN gms ELSE NULL END)/100 AS interest_query_gms,
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
  COUNT(q.query_raw) AS query_count,
  COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN q.query_raw ELSE NULL END) AS interest_query_count,
  SUM(l.sessions) AS query_session_count,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN l.sessions ELSE NULL END) AS interest_query_sessions_count,
  SUM(l.gms)/100 AS query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN l.gms ELSE NULL END)/100 AS interest_query_gms
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
WHERE score >= 0.1
GROUP BY 1
)
SELECT
  bin,
  CASE WHEN concept_count >= 8 THEN 1 ELSE 0 END AS above_8_interests,
  COUNT(*) AS query_count
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics` l
LEFT JOIN cte q 
  ON q.query = l.query
GROUP BY 1,2
;
