------------------------|| TAXONOMY LEVEL DISTRIBUTION ||------------------------
SELECT
top_category,
COUNT(listing_id) AS total_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 0 THEN listing_id ELSE NULL END) AS taxo_level_0_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 1 THEN listing_id ELSE NULL END) AS taxo_level_1_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 2 THEN listing_id ELSE NULL END) AS taxo_level_2_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 3 THEN listing_id ELSE NULL END) AS taxo_level_3_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 4 THEN listing_id ELSE NULL END) AS taxo_level_4_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 5 THEN listing_id ELSE NULL END) AS taxo_level_5_listing_count,
COUNT(CASE WHEN t.taxonomy_level = 6 THEN listing_id ELSE NULL END) AS taxo_level_6_listing_count,
SUM(past_year_gms) AS total_gms,
SUM(CASE WHEN t.taxonomy_level = 0 THEN past_year_gms ELSE NULL END) AS taxo_level_0_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 1 THEN past_year_gms ELSE NULL END) AS taxo_level_1_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 2 THEN past_year_gms ELSE NULL END) AS taxo_level_2_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 3 THEN past_year_gms ELSE NULL END) AS taxo_level_3_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 4 THEN past_year_gms ELSE NULL END) AS taxo_level_4_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 5 THEN past_year_gms ELSE NULL END) AS taxo_level_5_listing_gms,
SUM(CASE WHEN t.taxonomy_level = 6 THEN past_year_gms ELSE NULL END) AS taxo_level_6_listing_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l 
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
   ON l.taxonomy_id = t.taxonomy_id
-- WHERE is_seller = 1
GROUP BY 1
;

-- create a table that has taxonomy_id that are only at deepest leaf node / second deepst leaf node
BEGIN
CREATE TEMP TABLE leaf_node_level AS(
WITH RECURSIVE descendants AS(
  SELECT 
      parent_taxonomy_id AS parent, 
      taxonomy_id AS descendant, 
      1 AS level
   FROM `etsy-data-warehouse-prod.structured_data.taxonomy_latest`
   WHERE parent_taxonomy_id IS NOT NULL 
         -- AND is_seller = 1
         -- AND full_path LIKE 'home_and_living%'
  UNION ALL
   SELECT 
      d.parent, 
      s.taxonomy_id AS descendant,
      d.level + 1
   FROM descendants AS d
   JOIN `etsy-data-warehouse-prod.structured_data.taxonomy_latest` s
      ON d.descendant = s.parent_taxonomy_id
   WHERE s.parent_taxonomy_id IS NOT NULL 
         -- AND is_seller = 1
   --   AND s.full_path LIKE 'home_and_living%'
)
SELECT 
*
FROM descendants
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` AS (
WITH deepst_node AS(
SELECT 
  descendant AS taxonomy_id,
  'deepest' AS flag
FROM leaf_node_level
WHERE descendant NOT IN (SELECT parent FROM leaf_node_level)
)
SELECT
DISTINCT parent_taxonomy_id AS taxonomy_id,
'second deepest' AS flag
FROM `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
JOIN deepst_node d
    ON d.taxonomy_id = t.taxonomy_id
UNION ALL
SELECT
  DISTINCT descendant AS taxonomy_id,
  'deepest' AS flag
FROM leaf_node_level
WHERE descendant NOT IN (SELECT parent FROM leaf_node_level)
)
;

END
;

-- get deepest or second deepst node listing coverage
SELECT
top_category,
COUNT(l.listing_id) AS total_listing_count,
COUNT(CASE WHEN l.taxonomy_id IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` WHERE flag = 'deepest') THEN listing_id ELSE NULL END) AS deepest_node_listing_count,
COUNT(CASE WHEN l.taxonomy_id IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` WHERE flag = 'second deepest') THEN listing_id ELSE NULL END) AS second_deepest_node_listing_count,
SUM(past_year_gms) AS total_past_year_gms,
SUM(CASE WHEN l.taxonomy_id IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` WHERE flag = 'deepest') THEN past_year_gms ELSE NULL END) AS deepest_node_listing_gms,
SUM(CASE WHEN l.taxonomy_id IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` WHERE flag = 'second deepest') THEN past_year_gms ELSE NULL END) AS second_deepest_node_listing_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l 
GROUP BY 1
;

--Check when were non-deepst/second deepest node listings created and source
SELECT
source,
is_customshop,
EXTRACT(YEAR FROM original_create_date) as original_create_year,
count(listing_id)
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id NOT IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes`) and top_category = 'home_and_living'
GROUP BY 1,2,3
ORDER BY 4 DESC
;

SELECT
a.*,
t.*
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t USING(taxonomy_id)
WHERE taxonomy_id NOT IN (SELECT taxonomy_id FROM `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes`) and top_category = 'home_and_living'
;

------------------------|| BREATH & DEPTH OF TAXONOMY ||------------------------
-- DEPTH
SELECT
SPLIT(full_path, '.')[offset(0)] AS top_category,
taxonomy_level,
count(taxonomy_id) as taxo_count,
FROM `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
-- WHERE is_seller = 1
GROUP BY 1,2
ORDER BY 1,2
;

-- BREADTH
WITH siblings AS(
   SELECT
      SPLIT(full_path, '.')[offset(0)] AS top_category,
      parent_taxonomy_id,
      COUNT(DISTINCT taxonomy_id) AS number_of_sibling_count,
   FROM `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
   WHERE is_seller = 1
   GROUP BY 1,2
)
SELECT
top_category,
number_of_sibling_count,
COUNT(parent_taxonomy_id) AS parent_count
FROM siblings
GROUP BY 1,2
;

--pull example of parent nodes that has too many / little children nodes
WITH cte as (
SELECT
      taxonomy_id,
      parent_taxonomy_id,
      full_path,
      COUNT(DISTINCT taxonomy_id) OVER(PARTITION BY parent_taxonomy_id) AS sibling_count,
FROM `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
WHERE t.full_path LIKE 'home_and_living%' AND is_seller = 1
ORDER BY full_path
)
SELECT
DISTINCT cte.parent_taxonomy_id,
t.full_path,
t.taxonomy_level,
cte.sibling_count as children_count,
is_seller,
is_buyer
FROM cte
   JOIN `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
   ON cte.parent_taxonomy_id = t.taxonomy_id
-- WHERE is_seller = 1
ORDER BY 4 DESC,3
;
------------------------|| ATTRIBUTE UTILIZATION RATE ||------------------------
SELECT
la.attribute_id,
a.name,
la.is_set,
COUNT(la.listing_id) as listing_count,
SUM(l.past_year_gms) as gms
FROM `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` la
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` l
   ON la.listing_id = l.listing_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
   ON la.taxonomy_id = t.taxonomy_id
JOIN `etsy-data-warehouse-prod.structured_data.attributes_live` a
   ON la.attribute_id = a.attribute_id
WHERE t.full_path LIKE 'home_and_living%' AND is_seller = 1
GROUP BY 1,2,3
;


