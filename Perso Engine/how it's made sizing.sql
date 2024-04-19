-- listings, sellers, listing views, transactions, GMS
-- Seller made / Handmade By
-- 
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` a 
WHERE a.attribute_id = 175271657 AND a.attribute_value IN ("i_did","collective")
)
SELECT
"Seller made" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` a 
WHERE a.attribute_id = 175271657 AND a.attribute_value IN ("i_did","collective")
)
SELECT
"Seller made" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` a 
WHERE a.attribute_id = 175271657 AND a.attribute_value IN ("i_did","collective")
)
SELECT
"Seller made" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- vintage
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.materialized.listing_marketplaces` a 
WHERE a.is_vintage = 1
)
SELECT
"Vintage" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.materialized.listing_marketplaces` a 
WHERE a.is_vintage = 1
)
SELECT
"Vintage" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.materialized.listing_marketplaces` a 
WHERE a.is_vintage = 1
)
SELECT
"Vintage" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Seller sourced and curated
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Seller sourced and curated" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Seller sourced and curated" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Seller sourced and curated" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Creative Supplies / Sourced by
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools","paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools","paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools","paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Seller Designed
WITH def as (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
UNION ALL
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id = 2078
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
UNION ALL
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id = 2078
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
UNION ALL
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id = 2078
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Seller Design 
-- https://github.com/etsy-dev/cburke/blob/14be17e2eae2bef99f4636c03a0bebfe2ce61e98/quality/print_on_demand.sql#L146
WITH def as (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%" 
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
)
SELECT
"Seller Design" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;
