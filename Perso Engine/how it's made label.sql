------------------------------------------------------------------------------------------------------------------------------------------------------------
-- REPLICATE CURRENT PRODUCT LABELS LOGIC
------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` AS (
WITH production_partner_listings AS (
SELECT DISTINCT 
  sppla.listing_id,
  1 AS value
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
WHERE sppla.is_deleted = 0 AND spp.is_deleted = 0
)
, labels AS (
SELECT
CASE WHEN is_digital>=1 THEN 1 ELSE 0 END AS is_digital,
COALESCE(CAST(b.value AS INT64),0) AS is_supplies,
COALESCE(CAST(c.value AS INT64),0) AS is_vintage,
d.value AS is_handmade,
COALESCE(e.value ,0) AS has_production_partner,
a.listing_id,
a.past_year_gms,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN (SELECT listing_id,concept.value FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments` WHERE concept.type = "is_supplies") b ON a.listing_id = b.listing_id
LEFT JOIN (SELECT listing_id,concept.value FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments` WHERE concept.type = "is_vintage") c ON a.listing_id = c.listing_id
LEFT JOIN (SELECT listing_id,attribute_value as value FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` WHERE attribute_id = 175271657) d ON a.listing_id = d.listing_id
LEFT JOIN production_partner_listings e ON a.listing_id = e.listing_id
)
,backend_labels AS (
SELECT
is_digital,
is_supplies,
is_vintage,
is_handmade,
has_production_partner,
CASE WHEN is_handmade IN ("someone_else") AND is_supplies = 0 AND is_vintage = 0 AND has_production_partner = 1 THEN "Handmade with production assistance"
     WHEN is_handmade IN ("someone_else") AND is_supplies = 0 AND is_vintage = 1 AND has_production_partner = 0 THEN "Vintage"
     WHEN is_handmade IN ("someone_else") AND is_supplies = 1 AND is_vintage = 1 AND has_production_partner = 0 THEN "Vintage Supply"
     WHEN is_handmade IN ("i_did","collective") AND is_supplies = 0 AND is_vintage = 1 AND has_production_partner = 0 THEN "Handmade and Vintage"
     WHEN is_handmade IN ("i_did","collective") AND is_supplies = 1 AND is_vintage = 1 AND has_production_partner = 0 THEN "Handmade, Vintage Supply"
     WHEN is_handmade IN ("i_did","collective") AND is_supplies = 0 AND is_vintage = 0 THEN "Handmade"
     WHEN is_handmade IN ("i_did","collective") AND is_supplies = 1 AND is_vintage = 0 THEN "Handmade Supply"
     WHEN is_handmade IN ("someone_else") AND is_supplies = 1 AND is_vintage = 0 THEN "Supply"
     ELSE "N/A" END AS back_end_labels,
listing_id,
past_year_gms
FROM labels
)
SELECT
*,
CASE WHEN is_digital = 0 AND ((lower(back_end_labels) LIKE "%handmade%") OR is_handmade IN ("i_did","collective")) THEN 1 ELSE 0 END AS show_handmade,
CASE WHEN is_vintage = 1 THEN 1 ELSE 0 END AS show_vintage,
FROM backend_labels
);

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- CREATE LOGIC FOR NEW LABELS
------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_labels` AS (
-- NATURE ITEM LISTINGS
WITH him_seller_sourced_and_curated AS (
SELECT
  DISTINCT 
  "Seller sourced and curated" AS label,
  listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (6648,9125,6524,6516,6430,6440,6861,6231,6350,6362,6355,6646,6645,6357,6647,6358,6359,6650,6652,6649,6653,6651,6360,6361,6658,6655,9325,6660,6656,6657,6654,6661,6659,1893,6881,6879,1120,1121,1122,6882,1123,1124,1125,1126,6871,6883,6868,6886,1129,1132,7051,7050,6890,6889,6888,1140,1960,1959,1958,1158)
)
-- GIFT BOX
,him_seller_giftbox AS (
SELECT
  DISTINCT 
  "Giftbox" AS label,
  listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (322,207,11259,11256,11270)
)
-- # POD PARTNER LISTINGS
,him_pod_listing AS (
SELECT 
  DISTINCT
  listing_id,
  "POD" AS label
FROM `etsy-data-warehouse-prod.etsy_shard.suppression_listing_restrictions`
WHERE restriction = "originality-pod"
)
SELECT
  CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
  CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
  CASE WHEN d.label IS NOT NULL THEN 1 ELSE 0 END AS is_pod,
  CASE WHEN aa.is_digital>=1 THEN 1 ELSE 0 END AS is_digital,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` aa ON a.listing_id = aa.listing_id
LEFT JOIN him_seller_sourced_and_curated b ON a.listing_id = b.listing_id
LEFT JOIN him_seller_giftbox c on a.listing_id = c.listing_id
LEFT JOIN him_pod_listing d on a.listing_id = d.listing_id
)
;
