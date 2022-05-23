# CREATE TABLE THAT CONSOLIDATE ALL KB CONCEPTS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` AS (
WITH listing_concepts AS (
SELECT
listing_id,
'Category Full Path' as concept_domain,
full_path as value
FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
UNION ALL
SELECT
pc.listing_id,
'Perso Custo' AS concept_domain,
pc.full_label AS value
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` pc
WHERE pc.full_label != 'No Label'
UNION ALL 
SELECT
li.listing_id,
li.attribute_type AS conecept_domain,
display_name AS value
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests` li
WHERE _date = date_sub(current_date(), INTERVAL 1 DAY) AND score >=0.05 
),
active_listings AS (
SELECT
l.listing_id,
l.concept_domain,
l.value
FROM listing_concepts l
WHERE listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`)
),
concept_listing_count AS (
SELECT
concept_domain,
value,
count(distinct listing_id) AS listing_count
FROM active_listings
GROUP BY 1,2
),
rareness AS (
SELECT
distinct concept_domain,
value,
ntile(100) OVER (PARTITION BY concept_domain ORDER BY coalesce(listing_count,0) DESC) as concept_percentile,
ntile(20) OVER (PARTITION BY concept_domain ORDER BY coalesce(listing_count,0) DESC) as concept_vigintile,
ntile(10) OVER (PARTITION BY concept_domain ORDER BY coalesce(listing_count,0) desc) as concept_decile,
ntile(5) OVER (PARTITION BY concept_domain ORDER BY coalesce(listing_count,0) DESC) as concept_quintile
FROM concept_listing_count
)
SELECT
lc.*,
r.concept_percentile,
r.concept_vigintile,
r.concept_decile,
r.concept_quintile,
COUNT(DISTINCT lc.value) OVER (PARTITION BY listing_id) AS value_count,
AVG(concept_percentile) OVER (PARTITION BY listing_id) AS avg_percentile,
AVG(concept_vigintile) OVER (PARTITION BY listing_id) AS avg_vigintile,
AVG(concept_decile) OVER (PARTITION BY listing_id) AS avg_decile,
AVG(concept_quintile) OVER (PARTITION BY listing_id) AS avg_quintile,
MIN(concept_percentile) OVER (PARTITION BY listing_id) AS min_percentile,
MIN(concept_vigintile) OVER (PARTITION BY listing_id) AS min_vigintile,
MIN(concept_decile) OVER (PARTITION BY listing_id) AS min_decile,
MIN(concept_quintile) OVER (PARTITION BY listing_id) AS min_quintile,
MAX(concept_percentile) OVER (PARTITION BY listing_id) AS max_percentile,
MAX(concept_vigintile) OVER (PARTITION BY listing_id) AS max_vigintile,
MAX(concept_decile) OVER (PARTITION BY listing_id) AS max_decile,
MAX(concept_quintile) OVER (PARTITION BY listing_id) AS max_quintile,
PERCENTILE_CONT(concept_percentile, 0.5) OVER (PARTITION BY listing_id) AS med_percentile,
PERCENTILE_CONT(concept_vigintile, 0.5) OVER (PARTITION BY listing_id) AS med_vigintile,
PERCENTILE_CONT(concept_decile, 0.5) OVER (PARTITION BY listing_id) AS med_decile,
PERCENTILE_CONT(concept_quintile, 0.5) OVER (PARTITION BY listing_id) AS med_quintile
FROM active_listings lc
JOIN rareness r ON lc.concept_domain = r.concept_domain AND lc.value = r.value
)
;

-- ACTIVE LISTING COVERAGE 
SELECT
concept_domain,
COUNT(distinct listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` 
GROUP BY 1
ORDER BY 2 DESC
;

-- VALUE COUNT
SELECT
value_count,
COUNT(distinct listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` 
GROUP BY 1
ORDER BY 2 DESC
;
############################# HYPO 1: AVG PERCENTILE #############################
-- DISTRIBUTION
SELECT
ROUND(avg_percentile,0) AS round_avg_percentile,
COUNT(DISTINCT value) AS concept_count,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness`
GROUP BY 1
ORDER BY 1 ASC
;

-- TAKE SAMPLE
SELECT listing_id, avg_percentile FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(avg_percentile,0) <= 10 limit 50;
SELECT listing_id, avg_percentile FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(avg_percentile,0) >= 50 limit 50;

############################# HYPO 2: MEDIAN PERCENTILE #############################
-- DISTRIBUTION
SELECT
ROUND(med_percentile,0) AS round_med_percentile,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness`
GROUP BY 1
ORDER BY 1 ASC
;

-- TAKE SAMPLE
SELECT listing_id, avg_percentile  FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(med_percentile,0) <= 10 limit 50;
SELECT listing_id, avg_percentile  FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(med_percentile,0) >= 50 limit 50;

############################# HYPO 3: MAX PERCENTILE #############################
-- DISTRIBUTION
SELECT
ROUND(max_percentile,0) AS round_med_percentile,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness`
GROUP BY 1
ORDER BY 1 ASC
;

-- TAKE SAMPLE
SELECT listing_id, max_percentile  FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(med_percentile,0) <= 10 limit 50;
SELECT listing_id, max_percentile  FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` WHERE ROUND(med_percentile,0) >= 50 limit 50;

############################# HYPO 4: COMBO UNIQUNESS #############################
-- CREATE COMBO IN ARRAY
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_concept_combo` AS (
SELECT
listing_id,
array_agg(DISTINCT concept_domain ORDER BY concept_domain) as domain_combo,
array_agg(DISTINCT value ORDER BY value) as value_combo,
count(distinct concept_domain) as domain_count,
count(distinct value) as value_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` 
GROUP BY 1
)
;

--DOMAIN COMBO
SELECT 
ARRAY_TO_STRING(domain_combo," | ") as domain_combo,
count(listing_id) as listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concept_combo` 
GROUP BY 1
ORDER BY 2 DESC
;

-- VALUE COMBO
SELECT 
ARRAY_TO_STRING(value_combo," | "),
count(listing_id)
FROM `etsy-data-warehouse-dev.nlao.listing_concept_combo`
WHERE value_count > 1 
GROUP BY 1
ORDER BY 2 DESC
;



--CONCEPT UNIQUENESS
SELECT
concept_domain,
value,
concept_percentile,
concept_vigintile,
concept_decile,
concept_quintile
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness`
;







