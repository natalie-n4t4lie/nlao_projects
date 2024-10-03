CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.label_box_candidate_listings` AS (
-- get listing in desired category
WITH target_category_listings AS(
SELECT
  a.listing_id,
  b.taxonomy_id,
  b.full_path
FROM 
  `etsy-data-warehouse-prod.rollups.active_listing_basics` a
JOIN 
  `etsy-data-warehouse-prod.structured_data.taxonomy` b
    ON a.taxonomy_id = b.taxonomy_id
WHERE 
  b.full_path LIKE "home_and_living.bedding.blankets_and_throws%"
  OR b.full_path LIKE "home_and_living.bedding.duvet_covers%"
  OR b.full_path LIKE "home_and_living.bedding.sheets_and_pillowcases%"
)
-- calculate recs impression
, recs_impression AS (
SELECT
  a.listing_id,
  SUM(seen) AS recs_impression_ct
FROM target_category_listings a
LEFT JOIN 
  `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` b
  ON a.listing_id = b.listing_id
WHERE b._date >= CURRENT_DATE - 30
GROUP BY ALL
)
-- calculate search impression
, search_impression AS (
SELECT
	a.listing_id,
	SUM(impressions) AS search_impression_ct
FROM target_category_listings a
LEFT JOIN 
  `etsy-data-warehouse-prod.search.visit_level_listing_impressions` b
  ON a.listing_id = b.listing_id
WHERE
		_date >= CURRENT_DATE - 30
		AND DATE(TIMESTAMP_SECONDS(run_date)) >= CURRENT_DATE - 30
GROUP BY ALL
)
-- calculate ads impression
, ads_impression AS (
SELECT 
  a.listing_id,
  COUNT(*) AS ads_impression_ct
FROM target_category_listings a
LEFT JOIN `etsy-prolist-etl-prod.prolist.ads_impressions_*` b
  ON a.listing_id = b.listing_id
WHERE
    DATE(_PARTITIONTIME) >= CURRENT_DATE - 30 
GROUP BY ALL
)
-- aggregate all impression
, aggregate_impression AS (
SELECT
a.*,
COALESCE(b.recs_impression_ct,0) AS recs_impression_ct,
COALESCE(c.search_impression_ct,0) AS search_impression_ct,
COALESCE(d.ads_impression_ct,0) AS ads_impression_ct
FROM target_category_listings a
LEFT JOIN recs_impression b
  ON a.listing_id = b.listing_id
LEFT JOIN search_impression c
  ON a.listing_id = c.listing_id
LEFT JOIN ads_impression d
  ON a.listing_id = d.listing_id
)
-- choose listing with 10+ impression in the past 30 days
, filterout_low_impression_listings AS (
SELECT
  listing_id,
  taxonomy_id,
  full_path,
  recs_impression_ct,
  search_impression_ct,
  ads_impression_ct
FROM aggregate_impression
WHERE recs_impression_ct + search_impression_ct + ads_impression_ct >=10
)
-- get list of desired attriutes for listings above
, adoptable_attribute AS (
SELECT
SPLIT(a.full_path, ".")[safe_offset(0)] as level1,
SPLIT(a.full_path, ".")[safe_offset(1)] as level2,
SPLIT(a.full_path, ".")[safe_offset(2)] as level3,
a.taxonomy_id,
a.listing_id,
b.attribute_id,
b.is_set
FROM filterout_low_impression_listings a
JOIN `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` b
  ON a.listing_id = b.listing_id
WHERE 
  (SPLIT(a.full_path, ".")[safe_offset(2)] = "blankets_and_throws" AND attribute_id IN (2,3,4,55,68,185,271,342,345,356,357,390,691,739))
  OR 
  (SPLIT(a.full_path, ".")[safe_offset(2)] = "sheets_and_pillowcases" AND attribute_id IN (2,3,4,55,68,185,271,342,344,345,356,357,739))
  OR
  (SPLIT(a.full_path, ".")[safe_offset(2)] = "duvet_covers" AND attribute_id IN (2,3,4,185,271,342,344,345,356,357,739))
)
, variation_value AS (
SELECT
  a.listing_id,
  c.ott_attribute_id,
  c.attribute_name,
  c.attribute_value,
  d.product_id,
  c.scale_name
FROM adoptable_attribute a
JOIN `etsy-data-warehouse-prod.structured_data.attributes` aa
  ON a.attribute_id = aa.attribute_id
JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` c
  ON a.listing_id = c.listing_id AND a.attribute_id = c.ott_attribute_id AND lower(c.attribute_name) = lower(aa.name) AND c.is_active = 1
JOIN `etsy-data-warehouse-prod.etsy_shard.product_variations` d
  ON c.listing_id = d.listing_id AND c.listing_variation_id = d.listing_variation_id AND d.state = 1 
)
-- get attribute value from normal input and variation; for attribute value getting from variation, we will get product_id for it as well
SELECT
  a.level1,
  a.level2,
  a.level3,
  a.taxonomy_id,
  a.listing_id,
  CASE WHEN a.is_set = 1 THEN 1 ELSE 0 END AS value_from_attribute,
  CASE WHEN c.attribute_value IS NOT NULL THEN 1 ELSE 0 END AS value_from_variation,
  product_id,
  a.attribute_id,
  COALESCE(lower(aa.name),b.attribute_name, c.attribute_name) AS attribute_name,
  COALESCE(b.attribute_value, c.attribute_value) AS attribute_value,
  COALESCE(b.scale_name, c.scale_name) AS scale_name
FROM adoptable_attribute a
JOIN `etsy-data-warehouse-prod.structured_data.attributes` aa
  ON a.attribute_id = aa.attribute_id
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` b
  ON a.listing_id = b.listing_id AND a.attribute_id = b.attribute_id AND lower(b.attribute_name) = lower(aa.name) AND b.is_active = 1
LEFT JOIN variation_value c
  ON a.listing_id = c.listing_id AND a.attribute_id = c.ott_attribute_id AND lower(c.attribute_name) = lower(aa.name)
)
;

select
value_from_attribute,
value_from_variation,
case when scale_name is not null then 1 else 0 end as has_scale,
count(*)
FROM `etsy-data-warehouse-dev.nlao.label_box_candidate_listings`
WHERE attribute_id = 55
GROUP BY ALL
;

SELECT
  level3,
  COUNT(DISTINCT listing_id) AS eligible_listing_ct,
  COUNT(DISTINCT listing_id) * 0.0125 AS sample_ct
FROM `etsy-data-warehouse-dev.nlao.label_box_candidate_listings`
GROUP BY ALL
;

--sample for sheets_and_pillowcases 
WITH sample_listings AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.label_box_candidate_listings`
WHERE level3 = "sheets_and_pillowcases"
ORDER BY RAND()
LIMIT 1317
)
SELECT
  level1,
  level2,
  level3,
  taxonomy_id,
  b.listing_id,
  product_id,
  attribute_id,
  attribute_name,
  attribute_value
FROM sample_listings a
JOIN `etsy-data-warehouse-dev.nlao.label_box_candidate_listings` b
  ON a.listing_id = b.listing_id
ORDER BY level3, taxonomy_id, b.listing_id, attribute_id,product_id
;
-- blankets_and_throws
WITH sample_listings AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.label_box_candidate_listings`
WHERE level3 = "blankets_and_throws"
ORDER BY RAND()
LIMIT 6421
)
SELECT
  level1,
  level2,
  level3,
  taxonomy_id,
  b.listing_id,
  product_id,
  attribute_id,
  attribute_name,
  attribute_value
FROM sample_listings a
JOIN `etsy-data-warehouse-dev.nlao.label_box_candidate_listings` b
  ON a.listing_id = b.listing_id
ORDER BY level3, taxonomy_id, b.listing_id, attribute_id,product_id
;

-- sample for duvet_covers
WITH sample_listings AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.label_box_candidate_listings`
WHERE level3 = "duvet_covers"
ORDER BY RAND()
LIMIT 396
)
SELECT
  level1,
  level2,
  level3,
  taxonomy_id,
  b.listing_id,
  product_id,
  attribute_id,
  attribute_name,
  attribute_value
FROM sample_listings a
JOIN `etsy-data-warehouse-dev.nlao.label_box_candidate_listings` b
  ON a.listing_id = b.listing_id
ORDER BY level3, taxonomy_id, b.listing_id, attribute_id,product_id
;

