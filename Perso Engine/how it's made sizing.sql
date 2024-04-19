-- # Seller designed digital
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes` a 
WHERE a.is_digital = 1
)
SELECT
"Seller designed digital" AS label,
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
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes` a 
WHERE a.is_digital = 1
)
SELECT
"Seller designed digital" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Transaction
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes` a 
WHERE a.is_digital = 1
)
SELECT
"Seller designed digital" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Sample
SELECT
"Seller designed digital" AS label
,listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes` a 
WHERE a.is_digital = 1
ORDER BY RAND()
LIMIT 20
;

------------------------------------------------------------------------------------------------------------------------------------

-- vintage
-- listing count, gms, and seller count
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

-- listing view
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

-- transaction
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

-- sample
SELECT
"Vintage" AS label
,listing_id
FROM `etsy-data-warehouse-prod.materialized.listing_marketplaces` a 
WHERE a.is_vintage = 1 
ORDER BY RAND()
LIMIT 20
;

------------------------------------------------------------------------------------------------------------------------------------

-- Seller sourced and curated
-- listing count, gms, and seller count
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

-- listing view
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

-- transaction
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

-- sample
SELECT
"Seller sourced and curated" AS label
,listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
ORDER BY RAND()
LIMIT 20
;

------------------------------------------------------------------------------------------------------------------------------------

-- Creative Supplies / Sourced by (Craft)
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Craft)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

-- listing view
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Craft)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Craft)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- sample
SELECT
"Creative Supplies / Sourced by (Craft)" AS label
,listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
ORDER BY RAND()
LIMIT 20
;

------------------------------------------------------------------------------------------------------------------------------------

-- Creative Supplies / Sourced by (Paper)
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Paper)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_pct,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN listing_id IN (SELECT * FROM def) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN listing_id IN (SELECT * FROM def) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

-- listing view
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Paper)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
SELECT
"Creative Supplies / Sourced by (Paper)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- sample
SELECT
"Creative Supplies / Sourced by (Paper)" AS label
,listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
ORDER BY RAND()
LIMIT 20
;
