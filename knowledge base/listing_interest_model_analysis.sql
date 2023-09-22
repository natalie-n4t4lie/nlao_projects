SELECT *
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` 
WHERE _date = "2023-02-01" AND listing_id = 1221901151 
ORDER BY score DESC
LIMIT 1000
;

--SCORE DISTRIBUTION
SELECT
attribute_type,
CASE WHEN score <= 0.05 THEN '0.00-0.05'
     WHEN score <= 0.10 THEN '0.05-0.10'
     WHEN score <= 0.15 THEN '0.10-0.15'
     WHEN score <= 0.20 THEN '0.15-0.20'
     WHEN score <= 0.25 THEN '0.20-0.25'
     WHEN score <= 0.30 THEN '0.25-0.30'
     WHEN score <= 0.35 THEN '0.30-0.35'
     WHEN score <= 0.40 THEN '0.35-0.40'
     WHEN score <= 0.45 THEN '0.40-0.45'
     WHEN score <= 0.50 THEN '0.45-0.50'
     WHEN score <= 0.55 THEN '0.50-0.55'
     WHEN score <= 0.60 THEN '0.55-0.60'
     WHEN score <= 0.65 THEN '0.60-0.65'
     WHEN score <= 0.70 THEN '0.65-0.70'
     WHEN score <= 0.75 THEN '0.70-0.75'
     WHEN score <= 0.80 THEN '0.75-0.80'
     WHEN score <= 0.85 THEN '0.80-0.85'
     WHEN score <= 0.90 THEN '0.85-0.90'
     WHEN score <= 0.95 THEN '0.90-0.95'
     ELSE '0.95-1.00' 
     END AS score_bin,
COUNT(*) AS predictions
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` 
WHERE _date = CURRENT_DATE()
GROUP BY 1,2
;

-- MEAN
SELECT
  attribute_type,
  COUNT(DISTINCT listing_id) AS listing_count,
  COUNT(DISTINCT CASE WHEN score > 0.1 THEN listing_id ELSE NULL END) AS listing_count_01,
  COUNT(DISTINCT CASE WHEN score > 0.15 THEN listing_id ELSE NULL END) AS listing_count_015,
  COUNT(DISTINCT CASE WHEN score > 0.2 THEN listing_id ELSE NULL END) AS listing_count_02,
  COUNT(DISTINCT CASE WHEN score > 0.25 THEN listing_id ELSE NULL END) AS listing_count_025,
  avg(score) as average,
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2`
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE _date = CURRENT_DATE()
GROUP BY 1
ORDER BY 1 ASC
;

-- MEDIAN
SELECT
DISTINCT attribute_type,
PERCENTILE_CONT(score, 0.5) over(partition by attribute_type) as median,
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` 
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE _date = CURRENT_DATE()
ORDER BY 1 ASC
;

SELECT COUNT(listing_id) FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`;


-- recommended threshold 0.1, 0.15, 0.2, 0.25

-- Coverage by category 
SELECT
top_category,
COUNT(listing_id) AS active_listing_count,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.1 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_01,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.15 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_015,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.2 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_02,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.25 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_025,
SUM(past_year_gms) AS active_listing_gms,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.1 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_01,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.15 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_015,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.2 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_02,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.25 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_025,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
GROUP BY 1
;

-- LISTING VIEW COVERAGE
WITH listing_interest AS (
SELECT
listing_id,
attribute_type,
display_name
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2`
WHERE score >= 0.1 AND _date = CURRENT_DATE()
)
SELECT
COUNT(listing_id) AS active_listing_count,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.1 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_01,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.15 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_015,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.2 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_02,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.25 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_025
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) AND CURRENT_DATE()
; 

-- Category Coverage by interest type 
WITH cte AS (
SELECT
DISTINCT listing_id,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2`
WHERE score >= 0.1 AND _date = CURRENT_DATE()
)
SELECT
  l.top_category,
  c.attribute_type,
  COUNT(DISTINCT l.listing_id) AS listing_count,
  SUM(l.past_year_gms) AS gms_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
JOIN cte c USING (listing_id)
GROUP BY 1,2
;

SELECT
  attribute_type,
  COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2`
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE score >= 0.1 AND _date = CURRENT_DATE()
GROUP BY 1
;


--Number of interest per listings
WITH cte AS (
SELECT 
listing_id,
COUNT(DISTINCT CASE WHEN score >= 0.1 THEN display_name ELSE NULL END) AS interest_count_01,
COUNT(DISTINCT CASE WHEN score >= 0.15 THEN display_name ELSE NULL END) AS interest_count_015,
COUNT(DISTINCT CASE WHEN score >= 0.2 THEN display_name ELSE NULL END) AS interest_count_02,
COUNT(DISTINCT CASE WHEN score >= 0.25 THEN display_name ELSE NULL END) AS interest_count_025
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` 
WHERE _date = CURRENT_DATE()
GROUP BY 1
)
SELECT
CASE WHEN interest_count_01 IS NULL THEN 0
     WHEN interest_count_01 < 8 THEN interest_count_01 
     ELSE 8 END AS interest_count_01,
CASE WHEN interest_count_015 IS NULL THEN 0
     WHEN interest_count_015 < 8 THEN interest_count_015 
     ELSE 8 END AS interest_count_015,
CASE WHEN interest_count_02 IS NULL THEN 0
     WHEN interest_count_02 < 8 THEN interest_count_02 
     ELSE 8 END AS interest_count_02,
CASE WHEN interest_count_025 IS NULL THEN 0
     WHEN interest_count_025 < 8 THEN interest_count_025 
     ELSE 8 END AS interest_count_025,
COUNT(listing_id) AS listing_count,
SUM(past_year_gms) AS gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
LEFT JOIN cte USING (listing_id)
GROUP BY 1,2,3,4
;

--SELLER COUNTRY INTEREST
SELECT
CASE WHEN country_name IN ("United States","United Kingdom","France","Germany","Canada", "Australia", "India") THEN country_name ELSE "ROW" END AS country_name,
COUNT(listing_id) AS active_listing_count,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.1 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_01,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.15 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_015,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.2 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_02,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.25 AND _date = CURRENT_DATE()) THEN listing_id ELSE NULL END) AS listing_count_025,
SUM(past_year_gms) AS active_listing_gms,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.1 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_01,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.15 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_015,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.2 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_02,
SUM(CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` WHERE score >= 0.25 AND _date = CURRENT_DATE()) THEN past_year_gms ELSE NULL END) AS listing_gms_025,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
GROUP BY 1
;

-- INTEREST TYPE COMBO
WITH cte AS (
SELECT
DISTINCT listing_id,
attribute_type
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` USING(listing_id)
WHERE score >= 0.1 AND _date = CURRENT_DATE()
)
,combo_cal AS (
SELECT
listing_id,
string_agg(attribute_type, ", " ORDER BY attribute_type) AS attribute_combo,
-- OVER (PARTITION BY listing_id ORDER BY attribute_type) AS attribute_combo,
COUNT(DISTINCT attribute_type) AS attribute_type_count
-- OVER (PARTITION BY listing_id) AS attribute_type_count
FROM cte
GROUP BY 1
)
SELECT
attribute_type_count,
attribute_combo,
COUNT(listing_id) AS listing_count
FROM combo_cal
GROUP BY 1,2
;

-- INTEREST LABEL COMBO 
WITH cte AS (
SELECT
DISTINCT listing_id,
display_name
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2` USING(listing_id)
WHERE score >= 0.1 AND _date = CURRENT_DATE()
)
,combo_cal AS (
SELECT
listing_id,
string_agg(display_name, ", " ORDER BY display_name) AS interest_label_combo,
COUNT(DISTINCT display_name) AS interest_label_count
FROM cte
GROUP BY 1
)
SELECT
interest_label_count,
interest_label_combo,
COUNT(listing_id) AS listing_count
FROM combo_cal
GROUP BY 1,2
HAVING COUNT(listing_id)>1000
;

--% OF RECS DELIVERED WITH INTEREST MAPPED
WITH listing_interest AS (
SELECT
listing_id,
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests_v2`
WHERE score >= 0.1 AND _date = CURRENT_DATE()
)
SELECT
module_placement,
COUNT(visit_id) AS recs_delivered,
COUNT(CASE WHEN listing_id IN (SELECT listing_id FROM listing_interest) THEN visit_id ELSE NULL END) AS recs_delivered_w_interest
FROM `etsy-data-warehouse-prod.analytics.recsys_delivered_listings`
WHERE _date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
GROUP BY 1
ORDER BY 3 DESC
;




