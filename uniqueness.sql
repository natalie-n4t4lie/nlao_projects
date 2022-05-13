CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` AS (
WITH listing_concepts AS (
SELECT 
l.listing_id,
'Top Category' AS concept_domain,
l.top_category AS value,
FROM `etsy-data-warehouse-prod.listing_mart.listing_vw` l
UNION ALL
SELECT
listing_id,
'Category Full Path' as concept_domain,
full_path as value
FROM `etsy-data-warehouse-prod.materialized.listing_taxonomy`
UNION ALL
SELECT
listing_id,
'Category Path' as concept_domain,
path as value
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
WHERE _date = current_date() and score >=0.05 
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
r.concept_quintile
FROM active_listings lc
JOIN rareness r ON lc.concept_domain = r.concept_domain AND lc.value = r.value
)
;

--ACTIVE LISTING COVERAGE 
SELECT
concept_domain,
COUNT(distinct listing_id) AS listing_count
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` 
GROUP BY 1
ORDER BY 2 DESC
;

-- CONCEPT UNIQUENESS
SELECT
distinct concept_domain,
value,
concept_percentile
FROM `etsy-data-warehouse-dev.nlao.listing_concepts_uniqueness` 
;
