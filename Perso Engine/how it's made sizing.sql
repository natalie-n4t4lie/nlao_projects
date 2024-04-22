-- Seller made / Handmade By
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_seller_made` AS (
SELECT
DISTINCT 
"Seller made" AS label,
listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` a 
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE a.attribute_id = 175271657 AND a.attribute_value IN ("i_did","collective")
)
;
-- listings, sellers, listing views, transactions, GMS
SELECT
"Seller made" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_made` h
  USING (listing_id)
;

-- listing_view
SELECT
"Seller made" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_made` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Seller made" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_made` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_digital` AS (
SELECT
DISTINCT 
"Digital" AS label,
listing_id
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes` a 
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE a.is_digital = 1
)
;
-- listings, sellers, listing views, transactions, GMS
SELECT
"Digital" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_digital` h
  USING (listing_id)
;

-- listing_view
SELECT
"Digital" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_digital` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Digital" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_digital` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- vintage
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_vintage` AS (
SELECT
DISTINCT 
"Vintage" AS label,
listing_id
FROM `etsy-data-warehouse-prod.materialized.listing_marketplaces` a
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE a.is_vintage = 1
)
;

-- listings, sellers, listing views, transactions, GMS
SELECT
"Vintage" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` h
  USING (listing_id)
;

-- listing_view
SELECT
"Vintage" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Vintage" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Seller sourced and curated
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` AS (
SELECT
DISTINCT 
"Seller sourced and curated" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
;

-- listings, sellers, listing views, transactions, GMS
SELECT
"Seller sourced and curated" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` h
  USING (listing_id)
;

-- listing_view
SELECT
"Seller sourced and curated" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Seller sourced and curated" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;


-- Creative Supplies / Sourced by (Craft)
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` AS (
SELECT
DISTINCT 
"Creative Supplies / Sourced by(craft)" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
  AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
;

-- listings, sellers, listing views, transactions, GMS
SELECT
"Creative Supplies / Sourced by(craft)" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` h
  USING (listing_id)
;

-- listing_view
SELECT
"Creative Supplies / Sourced by(craft)" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Creative Supplies / Sourced by(craft)" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Creative Supplies / Sourced by(Paper)
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` AS (
SELECT
DISTINCT 
"Creative Supplies / Sourced by(paper)" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
  AND taxonomy_id NOT IN (1893,6231,6861,6628,1158,6890,6877,6881,6889,6888,1132,6879,1140,1120)
)
;

-- listings, sellers, listing views, transactions, GMS
SELECT
"Creative Supplies / Sourced by(paper)" AS label,
COUNT(DISTINCT h.listing_id) AS listing_ct,
COUNT(DISTINCT h.listing_id) / COUNT(l.listing_id) AS listing_pct,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) AS gms,
SUM(CASE WHEN h.listing_id IS NOT NULL THEN l.past_year_gms ELSE NULL END) / SUM(l.past_year_gms) AS gms_pct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) AS seller_ct,
COUNT(DISTINCT CASE WHEN h.listing_id IS NOT NULL THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS user_pct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` h
  USING (listing_id)
;

-- listing_view
SELECT
"Creative Supplies / Sourced by(paper)" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` h
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- transaction
SELECT
"Creative Supplies / Sourced by(paper)" AS label,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` h
  USING (listing_id)
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- # POD ACTIVE LISTINGS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_pod` AS (
WITH sppla_mapping AS (
SELECT DISTINCT 
  spp.shop_id,
  sppla.listing_id,
  spp.business_name,
  spp.descriptive_title,
  spp.about_production_partner,
  1 AS is_pod_partner_listing
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE LOWER(spp.business_name) LIKE "%print%"
  OR LOWER(spp.descriptive_title) LIKE "%print%"
  OR LOWER(spp.about_production_partner) LIKE "%print%"
),
api_users AS (
   SELECT user_id,
      api_key_id,
      COUNT(*) AS recs
   FROM `etsy-data-warehouse-prod.etsy_shard.user_api_keys`
   WHERE active = 1
   GROUP BY 1,2
   ORDER BY 1,2
   ),
apps AS (
   SELECT DISTINCT
   t.user_id,
   a.name,
   a.application_id,
   a.description,
   1 AS is_pod_api_shop
   FROM api_users t 
   JOIN `etsy-data-warehouse-prod.etsy_index.api_keys_index` k 
      ON t.api_key_id = k.api_key_id
   JOIN `etsy-data-warehouse-prod.etsy_shard.application_data` a 
      ON k.app_id = a.application_id
   JOIN `etsy-data-warehouse-prod.etsy_index.application_index` i 
      ON a.application_id = i.application_id
   WHERE a.name NOT IN ("Etsy for iPhone","Etsy for Android","Sell on Etsy for Android","Butter SOE (iOS)","Butter SOE (Android)")
      AND i.approved = 1 
      AND (LOWER(a.name) LIKE "%printful%" OR LOWER(a.name) LIKE "%printify%" OR LOWER(a.name) LIKE "%gooten%" OR LOWER(a.name) LIKE "%inkthreadable%")
      AND app_id IN (692758447517,1048119756481,182574138636,194131186944,826789741498,583596392566,404172100923,1021341745869,293514109927,908698012787,403922718747,513342174141,16100456054,1164547402740,914535233482,1153967920197,1112050276391,412564431162,1020017555556)
      --and i.state = 1
)
SELECT
l.listing_id,
p.is_pod_partner_listing,
a.is_pod_api_shop,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN sppla_mapping p ON l.listing_id = p.listing_id
LEFT JOIN apps a ON l.user_id = a.user_id);

-- # POD ACTIVE LISTINGS
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_partner_listing = 1
)  
SELECT
"Seller designed POD (Partner string match)" AS label,
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
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_partner_listing = 1
)  
SELECT
"Seller designed POD (Partner string match)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Transaction
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_partner_listing = 1
)  
SELECT
"Seller designed POD (Partner string match)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Sample
SELECT
"Seller designed POD (Partner string match)" AS label,
listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_partner_listing = 1
ORDER BY RAND()
LIMIT 20
;

--# Seller designed POD (API match)
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_api_shop = 1
)  
SELECT
"Seller designed POD (API match)" AS label,
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
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_api_shop = 1
)  
SELECT
"Seller designed POD (API match)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS listing_view_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS listing_view_pct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Transaction
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_api_shop = 1
)  
SELECT
"Seller designed POD (API match)" AS label,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) AS transaction_ct,
COUNT(CASE WHEN listing_id IN (SELECT * FROM def) THEN listing_id ELSE NULL END) / COUNT(*) AS transaction_pct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
WHERE date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
;

-- Sample
SELECT
"Seller designed POD (API match)" AS label,
listing_id
FROM `etsy-data-warehouse-dev.nlao.him_pod`
WHERE is_pod_api_shop = 1
ORDER BY RAND()
LIMIT 20
;

-- What's the overlaps between two POD segments
SELECT
is_pod_api_shop,
is_pod_partner_listing,
count(DISTINCT listing_id) AS ct
FROM `etsy-data-warehouse-dev.nlao.him_pod`
GROUP BY 1,2
;

-- How many % of "handmade" listings falls into either of these label?
SELECT
COUNT(DISTINCT a.listing_id) AS handmade,
COUNT(DISTINCT CASE WHEN aa.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS vintage,
COUNT(DISTINCT CASE WHEN b.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS digital,
COUNT(DISTINCT CASE WHEN c.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS sourced,
COUNT(DISTINCT CASE WHEN d.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS craft,
COUNT(DISTINCT CASE WHEN e.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS paper,
COUNT(DISTINCT CASE WHEN f.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS pod_string,
COUNT(DISTINCT CASE WHEN g.label IS NOT NULL THEN a.listing_id ELSE NULL END) AS pod_api,
FROM `etsy-data-warehouse-dev.nlao.him_seller_made` a
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_vintage` aa ON a.listing_id = aa.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_digital` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` c ON a.listing_id = c.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` d ON a.listing_id = d.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` e ON a.listing_id = e.listing_id
LEFT JOIN (SELECT "Seller designed POD (Partner string match)" AS label, listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod` WHERE is_pod_partner_listing = 1) f ON a.listing_id = f.listing_id
LEFT JOIN (SELECT "Seller designed POD (API match)" AS label, listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod` WHERE is_pod_api_shop = 1) g ON a.listing_id = g.listing_id
;

-- How many % of "handmade" listings falls into any label?
SELECT
COUNT(DISTINCT a.listing_id) AS handmade,
COUNT(DISTINCT 
CASE WHEN aa.label IS NOT NULL 
OR b.label IS NOT NULL 
OR c.label IS NOT NULL 
OR d.label IS NOT NULL 
OR e.label IS NOT NULL 
OR f.label IS NOT NULL 
OR g.label IS NOT NULL 
THEN a.listing_id ELSE NULL END) AS any_label,
FROM `etsy-data-warehouse-dev.nlao.him_seller_made` a
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_vintage` aa ON a.listing_id = aa.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_digital` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` c ON a.listing_id = c.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` d ON a.listing_id = d.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` e ON a.listing_id = e.listing_id
LEFT JOIN (SELECT "Seller designed POD (Partner string match)" AS label, listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod` WHERE is_pod_partner_listing = 1) f ON a.listing_id = f.listing_id
LEFT JOIN (SELECT "Seller designed POD (API match)" AS label, listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod` WHERE is_pod_api_shop = 1) g ON a.listing_id = g.listing_id
;
