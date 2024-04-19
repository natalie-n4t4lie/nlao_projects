-- # Seller designed non-POD
-- listing count, gms, and seller count
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
"Seller designed non-POD" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

--listing view
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
"Seller designed non-POD" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Transaction
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
"Seller designed non-POD" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Sample
SELECT
"Seller designed non-POD" AS label,
listing_id
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE
  LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
ORDER BY RAND()
LIMIT 20
;

