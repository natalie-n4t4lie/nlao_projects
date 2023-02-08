-- REFORMAT HTML ESCAPE QUERIES --
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.query_interests` AS (
SELECT
  REPLACE(REPLACE(query, '&quot;', '"'),"&#39;","'") AS clean_query,
  *
FROM `etsy-data-warehouse-dev.knowledge_base.query_interests_adhoc` i
WHERE _date <= '2023-01-20'
)
;

SELECT count(DISTINCT clean_query) FROM `etsy-data-warehouse-dev.nlao.query_interests`
where clean_query not in (SELECT query FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`)
;--104,622,480

SELECT count(DISTINCT clean_query) FROM `etsy-data-warehouse-dev.nlao.query_interests`
where clean_query in (SELECT query_raw FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`)
;--113,083,692

SELECT count(DISTINCT clean_query) FROM `etsy-data-warehouse-dev.nlao.query_interests`
where clean_query NOT in (SELECT query_raw FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`)
;--48,231,321

SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`
where clean_query not in (SELECT query_raw FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`)
limit 100
;

-- CHECK HOW MANY QUERY COVERAGE FOR HTML ESCAPE QUERIES
select
COUNT(DISTINCT CASE WHEN clean_query !=query THEN query ELSE NULL END) / count(DISTINCT query) AS html_queries_count_share,
COUNT(CASE WHEN clean_query !=query THEN query ELSE NULL END) / count(query) AS html_query_prediction_count_share
FROM `etsy-data-warehouse-dev.nlao.query_interests`
;

SELECT
bin,
COUNT(DISTINCT q.query_raw) AS query,
COUNT(DISTINCT CASE WHEN clean_query != qi.query THEN q.query_raw else null end) AS html_query
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` q
JOIN `etsy-data-warehouse-dev.nlao.query_interests` qi
  ON q.query_raw = qi.clean_query
GROUP BY 1
;

-- BASIC STATS FOR QUERY BIN
SELECT 
  bin,
  COUNT(DISTINCT query),
  SUM(gms)/100 as gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
GROUP BY 1
;

-- QUERY COUNT/GMS COVERAGE
-- QUERY BIN
SELECT 
  bin,
  COUNT(q.query_raw) AS query_count,
  COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN q.query_raw ELSE NULL END) AS interest_query_count,
  COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.1) THEN q.query_raw ELSE NULL END) AS interest_01_query_count,
    COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.15) THEN q.query_raw ELSE NULL END) AS interest_015_query_count,
    COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.2) THEN q.query_raw ELSE NULL END) AS interest_02_query_count,
  SUM(gms)/100 AS query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN gms ELSE NULL END)/100 AS interest_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.1) THEN gms ELSE NULL END)/100 AS interest_01_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.15) THEN gms ELSE NULL END)/100 AS interest_015_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.2) THEN gms ELSE NULL END)/100 AS interest_02_query_gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` q
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
  COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.1) THEN q.query_raw ELSE NULL END) AS interest_01_query_count,
    COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.15) THEN q.query_raw ELSE NULL END) AS interest_015_query_count,
    COUNT(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.2) THEN q.query_raw ELSE NULL END) AS interest_02_query_count,
  SUM(gms)/100 AS query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests`) THEN gms ELSE NULL END)/100 AS interest_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.1) THEN gms ELSE NULL END)/100 AS interest_01_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.15) THEN gms ELSE NULL END)/100 AS interest_015_query_gms,
  SUM(CASE WHEN q.query_raw IN (SELECT clean_query FROM `etsy-data-warehouse-dev.nlao.query_interests` WHERE score >=0.2) THEN gms ELSE NULL END)/100 AS interest_02_query_gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` q
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_intent_labels_dedupped` qi 
  ON qi.query_raw = q.query_raw
GROUP BY 1
;

-- INTEREST LABEL COUNT DISTRIBUTION
WITH cte AS (
SELECT
  clean_query,
  COUNT(DISTINCT display_name) AS concept_count,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN display_name ELSE NULL END) AS concept_01_count,
  COUNT(DISTINCT CASE WHEN score >=0.15 THEN display_name ELSE NULL END) AS concept_015_count,
  COUNT(DISTINCT CASE WHEN score >=0.2 THEN display_name ELSE NULL END) AS concept_02_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1
)
SELECT
  bin,
  CASE WHEN concept_count IS NULL THEN 0 
        WHEN concept_count >= 8 THEN 8 
        ELSE concept_count END AS concept_count,
  CASE WHEN concept_01_count IS NULL THEN 0 
        WHEN concept_01_count >= 8 THEN 8 
        ELSE concept_01_count END AS concept_01_count,
  CASE WHEN concept_015_count IS NULL THEN 0 
        WHEN concept_015_count >= 8 THEN 8 
        ELSE concept_015_count END AS concept_015_count,
  CASE WHEN concept_02_count IS NULL THEN 0 
        WHEN concept_02_count >= 8 THEN 8 
        ELSE concept_02_count END AS concept_02_count,
  COUNT(query_raw) AS query_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
LEFT JOIN cte q 
  ON q.clean_query = l.query_raw
GROUP BY 1,2,3,4,5
;

-- QUERY INTENT
WITH cte AS (
SELECT
  clean_query,
  COUNT(DISTINCT display_name) AS concept_count,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN display_name ELSE NULL END) AS concept_01_count,
  COUNT(DISTINCT CASE WHEN score >=0.15 THEN display_name ELSE NULL END) AS concept_015_count,
  COUNT(DISTINCT CASE WHEN score >=0.2 THEN display_name ELSE NULL END) AS concept_02_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1
)
SELECT
  CASE WHEN confidence >= 0.5 THEN label ELSE 'Unsure' END AS label,
  CASE WHEN concept_count IS NULL THEN 0 
        WHEN concept_count >= 5 THEN 5 
        ELSE concept_count END AS concept_count,
  CASE WHEN concept_01_count IS NULL THEN 0 
        WHEN concept_01_count >= 5 THEN 5
        ELSE concept_01_count END AS concept_01_count,
  CASE WHEN concept_015_count IS NULL THEN 0 
        WHEN concept_015_count >= 5 THEN 5 
        ELSE concept_015_count END AS concept_015_count,
  CASE WHEN concept_02_count IS NULL THEN 0 
        WHEN concept_02_count >= 5 THEN 5 
        ELSE concept_02_count END AS concept_02_count,
  COUNT(l.query_raw) AS query_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_intent_labels_dedupped` qi 
  ON qi.query_raw = l.query_raw
LEFT JOIN cte q 
  ON q.clean_query = l.query_raw
GROUP BY 1,2,3,4,5
;

-- # OF INTEREST TYPE MAPPED TO QUERY
WITH cte AS (
SELECT
  clean_query,
  COUNT(DISTINCT attribute_type) AS concept_type_count,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN attribute_type ELSE NULL END) AS concept_type_01_count,
  COUNT(DISTINCT CASE WHEN score >=0.15 THEN attribute_type ELSE NULL END) AS concept_type_015_count,
  COUNT(DISTINCT CASE WHEN score >=0.2 THEN attribute_type ELSE NULL END) AS concept_type_02_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1
)
SELECT
  bin,
  CASE WHEN concept_type_count IS NULL THEN 0 
        WHEN concept_type_count >= 5 THEN 5 
        ELSE concept_type_count END AS concept_type_count,
  CASE WHEN concept_type_01_count IS NULL THEN 0 
        WHEN concept_type_01_count >= 5 THEN 5
        ELSE concept_type_01_count END AS concept_type_01_count,
  CASE WHEN concept_type_015_count IS NULL THEN 0 
        WHEN concept_type_015_count >= 5 THEN 5 
        ELSE concept_type_015_count END AS concept_type_015_count,
  CASE WHEN concept_type_02_count IS NULL THEN 0 
        WHEN concept_type_02_count >= 5 THEN 5 
        ELSE concept_type_02_count END AS concept_type_02_count,
  COUNT(query_raw) AS query_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
LEFT JOIN cte q 
  ON q.clean_query = l.query_raw
GROUP BY 1,2,3,4,5
;

-- QUERY INTENT
WITH cte AS (
SELECT
  clean_query,
  COUNT(DISTINCT attribute_type) AS concept_type_count,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN attribute_type ELSE NULL END) AS concept_type_01_count,
  COUNT(DISTINCT CASE WHEN score >=0.15 THEN attribute_type ELSE NULL END) AS concept_type_015_count,
  COUNT(DISTINCT CASE WHEN score >=0.2 THEN attribute_type ELSE NULL END) AS concept_type_02_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1
)
SELECT
  CASE WHEN confidence >= 0.5 THEN label ELSE 'Unsure' END AS label,
  CASE WHEN concept_type_count IS NULL THEN 0 
        WHEN concept_type_count >= 5 THEN 5 
        ELSE concept_type_count END AS concept_type_count,
  CASE WHEN concept_type_01_count IS NULL THEN 0 
        WHEN concept_type_01_count >= 5 THEN 5
        ELSE concept_type_01_count END AS concept_type_01_count,
  CASE WHEN concept_type_015_count IS NULL THEN 0 
        WHEN concept_type_015_count >= 5 THEN 5 
        ELSE concept_type_015_count END AS concept_type_015_count,
  CASE WHEN concept_type_02_count IS NULL THEN 0 
        WHEN concept_type_02_count >= 5 THEN 5 
        ELSE concept_type_02_count END AS concept_type_02_count,
  COUNT(l.query_raw) AS query_count,
  SUM(l.gms)/100 AS query_gms,
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_intent_labels_dedupped` qi 
  ON qi.query_raw = l.query_raw
LEFT JOIN cte q 
  ON q.clean_query = l.query_raw
GROUP BY 1,2,3,4,5
;

-- INTEREST TYPE
SELECT
  attribute_type,
  display_name,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN attribute_type ELSE NULL END) AS concept_type_01_count,
  COUNT(DISTINCT CASE WHEN score >=0.15 THEN attribute_type ELSE NULL END) AS concept_type_015_count,
  COUNT(DISTINCT CASE WHEN score >=0.2 THEN attribute_type ELSE NULL END) AS concept_type_02_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1,2
;


--Interest type popularity by threshold
-- 0
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
) 
SELECT 
attribute_type,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1
ORDER BY 2 DESC
;

--0.1
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score>=0.1
) 
SELECT 
attribute_type,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1
ORDER BY 2 DESC
;

--0.15
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score>=0.15
) 
SELECT 
attribute_type,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1
ORDER BY 2 DESC
;

--0.2
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score>=0.2
) 
SELECT 
attribute_type,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1
ORDER BY 2 DESC
;

SELECT
attribute_type,
CASE WHEN score <= 0.1 THEN '0.0-0.1'
     WHEN score <= 0.2 THEN '0.1-0.2'
     WHEN score <= 0.3 THEN '0.2-0.3'
     WHEN score <= 0.4 THEN '0.3-0.4'
     WHEN score <= 0.5 THEN '0.4-0.5'
     WHEN score <= 0.6 THEN '0.5-0.6'
     WHEN score <= 0.7 THEN '0.6-0.7'
     WHEN score <= 0.8 THEN '0.7-0.8'
     WHEN score <= 0.9 THEN '0.8-0.9'
     ELSE '0.9-1.0' 
     END AS score_bin,
COUNT(*) AS predictions
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1,2
;


-- INTEREST TYPE OVERLAPS WITH TEXT MATCHING METHOD (HOLIDAY AND OCCASION)
WITH cte AS (
SELECT
  q.query_raw,
  is_holiday AS text_is_holiday,
  is_occasion AS text_is_occasion,
  MAX(CASE WHEN score >= 0.1 AND lower(attribute_type) = "holiday" THEN 1 ELSE 0 END) AS model_is_holiday,
  MAX(CASE WHEN score >= 0.1 AND lower(attribute_type) = "occasion" THEN 1 ELSE 0 END) AS model_is_occasion
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` q
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_interests` qi 
  ON q.query_raw = qi.clean_query
GROUP BY 1,2,3
)
SELECT
model_is_holiday,
model_is_occasion,
text_is_holiday,
text_is_occasion,
COUNT(query_raw) AS query_count
FROM cte
GROUP BY 1,2,3,4
;


--Interest label popularity by threshold
-- 0
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
display_name,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
) 
SELECT 
attribute_type,
display_name,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1,2
ORDER BY 3 DESC
;

--0.1
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
display_name,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score >=0.1
) 
SELECT 
attribute_type,
display_name,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1,2
ORDER BY 3 DESC
;

--0.15
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
display_name,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score >=0.15
) 
SELECT 
attribute_type,
display_name,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1,2
ORDER BY 3 DESC
;

--0.2
WITH cte AS (
SELECT
distinct l.query_raw,
attribute_type,
display_name,
gms/100 AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` l
JOIN `etsy-data-warehouse-dev.nlao.query_interests` q
  ON l.query_raw = q.clean_query
WHERE score >=0.2
) 
SELECT 
attribute_type,
display_name,
COUNT(query_raw) AS query_count,
SUM(gms) AS query_gms,
FROM cte
GROUP BY 1,2
ORDER BY 3 DESC
;



-- CHECK INTEREST LABEL COUNT WITHIN A VISIT
-- RANDOM DAY
WITH cte AS (
SELECT  
visit_id,
COUNT(DISTINCT query_raw) AS query_count,
COUNT(DISTINCT CASE WHEN score >=0.1 THEN display_name ELSE NULL END) AS concept_label_01_count,
COUNT(DISTINCT CASE WHEN score >=0.15 THEN display_name ELSE NULL END) AS concept_label_015_count,
COUNT(DISTINCT CASE WHEN score >=0.2 THEN display_name ELSE NULL END) AS concept_label_02_count,
FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_interests` qi
  ON q.query_raw = qi.clean_query
WHERE q._date = '2022-01-02' AND query_raw IS NOT NULL
GROUP BY 1
)
SELECT
CASE WHEN query_count >=5 THEN 5
  ELSE query_count 
  END AS query_count,
CASE WHEN concept_label_01_count >= 8 THEN 8
     WHEN concept_label_01_count BETWEEN 1 AND 7 THEN concept_label_01_count
     ELSE 0 END AS concept_label_01_count,
CASE WHEN concept_label_015_count >= 8 THEN 8
     WHEN concept_label_015_count BETWEEN 1 AND 7 THEN concept_label_015_count
     ELSE 0 END AS concept_label_015_count,
CASE WHEN concept_label_02_count >= 8 THEN 8
     WHEN concept_label_02_count BETWEEN 1 AND 7 THEN concept_label_02_count
     ELSE 0 END AS concept_label_02_count,
COUNT(visit_id) AS visit_count
FROM cte
GROUP BY 1,2,3,4
;

-- SEARCH INGRESS EXPERIMENT POWER ANALYSIS
  -- INTEREST LABEL >= 7 VISIT TRAFFIC
WITH cte AS (
SELECT
  clean_query,
  COUNT(DISTINCT CASE WHEN score >=0.1 THEN display_name ELSE NULL END) AS concept_01_count
FROM `etsy-data-warehouse-dev.nlao.query_interests`
GROUP BY 1
)
SELECT
  count(distinct visit_id) AS visit_traffic,
FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
JOIN cte c 
  ON q.query_raw = c.clean_query 
WHERE _date ='2022-01-01' AND concept_01_count>=7
;--17941
-- http://www.experimentcalculator.com/#lift=1&conversion=6.44&confidence=95&visits=17941&percentage=50&power=80

-- GROUPING LABELS TOGETHER WITHIN A VISIT, INTEREST LABEL >= 7 VISIT TRAFFIC
WITH cte AS (
SELECT  
visit_id,
COUNT(DISTINCT CASE WHEN score >=0.1 THEN display_name ELSE NULL END) AS concept_label_01_count,
FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
LEFT JOIN `etsy-data-warehouse-dev.nlao.query_interests` qi
  ON q.query_raw = qi.clean_query
WHERE q._date = '2022-01-01' AND query_raw IS NOT NULL
GROUP BY 1
)
SELECT
COUNT(visit_id) AS visit_count
FROM cte
WHERE concept_label_01_count >= 7
;-- 47909
  -- http://www.experimentcalculator.com/#lift=1&conversion=6.44&confidence=95&visits=47909&percentage=50&power=80

