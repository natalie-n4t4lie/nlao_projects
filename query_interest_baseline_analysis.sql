--Interest type popularity by threshold
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

