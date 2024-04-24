-- Vintage
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

-- Supplies
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` AS (
SELECT
DISTINCT 
"Creative Supplies / Sourced by(craft)" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("craft_supplies_and_tools")
  AND taxonomy_id NOT IN (6648,9125,6524,6516,6430,6440,6861,6231,6350,6362,6355,6646,6645,6357,6647,6358,6359,6650,6652,6649,6653,6651,6360,6361,6658,6655,9325,6660,6656,6657,6654,6661,6659,1893,6881,6879,1120,1121,1122,6882,1123,1124,1125,1126,6871,6883,6868,6886,1129,1132,7051,7050,6890,6889,6888,1140,1960,1959,1958,1158)
)
;

-- Paper Party Supplies
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` AS (
SELECT
DISTINCT 
"Creative Supplies / Sourced by(paper)" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE top_category IN ("paper_and_party_supplies")
  AND taxonomy_id NOT IN (6648,9125,6524,6516,6430,6440,6861,6231,6350,6362,6355,6646,6645,6357,6647,6358,6359,6650,6652,6649,6653,6651,6360,6361,6658,6655,9325,6660,6656,6657,6654,6661,6659,1893,6881,6879,1120,1121,1122,6882,1123,1124,1125,1126,6871,6883,6868,6886,1129,1132,7051,7050,6890,6889,6888,1140,1960,1959,1958,1158)
)
;

-- listings, sellers, listing views, transactions, GMS
SELECT
CASE WHEN a.label IS NOT NULL THEN 1 ELSE 0 END AS is_vintage,
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_craft,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_supply,
COUNT(DISTINCT l.listing_id) AS listing_ct,
SUM(l.past_year_gms) AS gms,
COUNT(DISTINCT l.user_id) AS seller_ct,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` a
  ON l.listing_id = a.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` b
  ON l.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` c
  ON l.listing_id = c.listing_id
GROUP BY 1,2,3
;

-- listing_view
SELECT
CASE WHEN a.label IS NOT NULL THEN 1 ELSE 0 END AS is_vintage,
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_craft,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_supply,
COUNT(*) AS listing_view_ct,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` a
  ON l.listing_id = a.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` b
  ON l.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` c
  ON l.listing_id = c.listing_id
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
GROUP BY 1,2,3
;

-- transaction
SELECT
CASE WHEN a.label IS NOT NULL THEN 1 ELSE 0 END AS is_vintage,
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_craft,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_supply,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions`
LEFT JOIN  `etsy-data-warehouse-dev.nlao.him_vintage` a
  ON l.listing_id = a.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_craft` b
  ON l.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_craft_supplies_paper` c
  ON l.listing_id = c.listing_id
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
GROUP BY 1,2,3
;

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- NATURE ITEM LISTINGS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` AS (
SELECT
DISTINCT 
"Seller sourced and curated" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (6648,9125,6524,6516,6430,6440,6861,6231,6350,6362,6355,6646,6645,6357,6647,6358,6359,6650,6652,6649,6653,6651,6360,6361,6658,6655,9325,6660,6656,6657,6654,6661,6659,1893,6881,6879,1120,1121,1122,6882,1123,1124,1125,1126,6871,6883,6868,6886,1129,1132,7051,7050,6890,6889,6888,1140,1960,1959,1958,1158)
)
;

-- # POD PARTNER LISTINGS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_pod_partner` AS (
WITH sppla_mapping AS (
SELECT DISTINCT 
  sppla.listing_id,
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
   aa.listing_id
   FROM api_users t 
   JOIN `etsy-data-warehouse-prod.etsy_index.api_keys_index` k 
      ON t.api_key_id = k.api_key_id
   JOIN `etsy-data-warehouse-prod.etsy_shard.application_data` a 
      ON k.app_id = a.application_id
   JOIN `etsy-data-warehouse-prod.etsy_index.application_index` i 
      ON a.application_id = i.application_id
   JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` aa
      ON t.user_id = aa.user_id
   WHERE a.name NOT IN ("Etsy for iPhone","Etsy for Android","Sell on Etsy for Android","Butter SOE (iOS)","Butter SOE (Android)")
      AND i.approved = 1 
      AND (LOWER(a.name) LIKE "%printful%" OR LOWER(a.name) LIKE "%printify%" OR LOWER(a.name) LIKE "%gooten%" OR LOWER(a.name) LIKE "%inkthreadable%")
      AND app_id IN (692758447517,1048119756481,182574138636,194131186944,826789741498,583596392566,404172100923,1021341745869,293514109927,908698012787,403922718747,513342174141,16100456054,1164547402740,914535233482,1153967920197,1112050276391,412564431162,1020017555556)
      --and i.state = 1
)
SELECT DISTINCT
listing_id
FROM sppla_mapping
UNION DISTINCT
SELECT DISTINCT
listing_id
FROM apps
);

-- # POD PARTNER LISTINGS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_non_pod_partner` AS (
SELECT DISTINCT 
  sppla.listing_id,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE listing_id NOT IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod_partner`)
);

-- gift box
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_seller_giftbox` AS (
SELECT
DISTINCT 
"Giftbox" AS label,
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (322,207,11259,11256,11270)
)
;


SELECT
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
CASE WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod_partner`) THEN "Has production partner, is POD"
     WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_non_pod_partner`) THEN "Has production partner, not POD"
     ELSE "No production partner" END AS production_partner_type,
aa.is_digital,
COUNT(DISTINCT a.listing_id) AS listing_ct,
SUM(a.past_year_gms) AS past_year_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` aa ON a.listing_id = aa.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_giftbox` c on a.listing_id = c.listing_id
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;

-- listing_view
SELECT
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
CASE WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod_partner`) THEN "Has production partner, is POD"
     WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_non_pod_partner`) THEN "Has production partner, not POD"
     ELSE "No production partner" END AS production_partner_type,
aa.is_digital,
COUNT(*) AS listing_view_ct,
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` aa ON a.listing_id = aa.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_giftbox` c on a.listing_id = c.listing_id
WHERE _date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;

-- transaction
SELECT
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
CASE WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod_partner`) THEN "Has production partner, is POD"
     WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_non_pod_partner`) THEN "Has production partner, not POD"
     ELSE "No production partner" END AS production_partner_type,
COUNT(CASE WHEN h.listing_id IS NOT NULL THEN listing_id ELSE NULL END) AS transaction_ct,
FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` aa ON a.listing_id = aa.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_giftbox` c on a.listing_id = c.listing_id
WHERE _date BETWEEN CURRENT_DATE - 366 AND CURRENT_DATE - 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;
