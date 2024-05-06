--// sql logic for https://docs.google.com/spreadsheets/d/10U5ZZpxCwXdKx71zX1ZGW4PxDicEoa9D2sP28UT6jho/edit#gid=905577962
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
BEGIN
-- NATURE ITEM LISTINGS
CREATE TEMP TABLE him_seller_sourced_and_curated AS (
SELECT
  DISTINCT 
  "Seller sourced and curated" AS label,
  listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (6648,9125,6524,6516,6430,6440,6861,6231,6350,6362,6355,6646,6645,6357,6647,6358,6359,6650,6652,6649,6653,6651,6360,6361,6658,6655,9325,6660,6656,6657,6654,6661,6659,1893,6881,6879,1120,1121,1122,6882,1123,1124,1125,1126,6871,6883,6868,6886,1129,1132,7051,7050,6890,6889,6888,1140,1960,1959,1958,1158)
);

-- GIFT BOX
CREATE TEMP TABLE him_seller_giftbox AS (
SELECT
  DISTINCT 
  "Giftbox" AS label,
  listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE taxonomy_id IN (322,207,11259,11256,11270)
);

-- # POD PARTNER LISTINGS
CREATE TEMP TABLE him_pod_listing AS (
SELECT 
  DISTINCT
  listing_id,
  "POD" AS label
FROM `etsy-data-warehouse-prod.etsy_shard.suppression_listing_restrictions`
WHERE restriction = "originality-pod"
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_labels` AS (
SELECT
  CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
  CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
  CASE WHEN d.label IS NOT NULL THEN 1 ELSE 0 END AS is_pod,
  CASE WHEN aa.is_digital>=1 THEN 1 ELSE 0 END AS is_digital,
  a.listing_id,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` aa ON a.listing_id = aa.listing_id
LEFT JOIN him_seller_sourced_and_curated b ON a.listing_id = b.listing_id
LEFT JOIN him_seller_giftbox c on a.listing_id = c.listing_id
LEFT JOIN him_pod_listing d on a.listing_id = d.listing_id
)
;
END;

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- JOIN CURRENT AND NEW LABEL TABLE
------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_joined_table` AS (
SELECT
a.is_digital,
a.is_supplies,
a.is_vintage,
a.is_handmade,
a.has_production_partner,
a.back_end_labels AS current_backend_labels,
a.show_handmade AS current_frontend_handmade,
a.show_vintage AS current_frontend_vintage,
b.is_nature_item,
b.is_giftbox,
b.is_pod,
--// variant A sizing
-- If digital = true
CASE WHEN 
        a.is_digital = 1 
      THEN "seller designed" 
-- Item from nature: If category = Any of these (static list for MVP)[Not digital, and Not Potential POD]
     WHEN b.is_nature_item = 1 
      THEN "seller sourced and curated: nature item"
-- Gift box: If category = Any of these (static list for MVP) [Not digital, and Not Potential POD]
     WHEN b.is_giftbox = 1 
      THEN "seller sourced and curated: giftbox"
-- “Who made it = someone else” handmade=(someone_else) [Not digital, Not Potential POD]
     WHEN a.is_handmade = "someone_else" 
      THEN "no label someone else"
-- handmade=(i_did, collective) [Not digital, Not Potential POD, not captured in above logic]
     WHEN a.is_handmade IN ("i_did","collective") 
      THEN "handmade"  
    ELSE "not assigned" 
END AS variant_a_label,
--// variant B sizing
  -- If digital = true
CASE WHEN 
        a.is_digital = 1 
      THEN "seller designed" 
-- Listing identified as potential POD listing in *nightly job [Not digital]
     WHEN b.is_pod = 1 
      THEN "no label pod"
-- Item from nature: If category = Any of these (static list for MVP)[Not digital, and Not Potential POD]
     WHEN b.is_nature_item = 1 
      THEN "seller sourced and curated: nature item"
-- Gift box: If category = Any of these (static list for MVP) [Not digital, and Not Potential POD]
     WHEN b.is_giftbox = 1 
      THEN "seller sourced and curated: giftbox"
-- “Who made it = someone else” handmade=(someone_else) [Not digital, Not Potential POD]
     WHEN a.is_handmade = "someone_else" 
      THEN "no label someone else"
-- handmade=(i_did, collective) [Not digital, Not Potential POD, not captured in above logic]
     WHEN a.is_handmade IN ("i_did","collective") 
      THEN "handmade"  
    ELSE "not assigned" 
END AS variant_b_label,
a.listing_id,
a.past_year_gms
FROM `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` a
JOIN `etsy-data-warehouse-dev.nlao.him_labels` b using (listing_id)
)
;

------------------------------------------------------------------------------------------------------------------------------------------------------------
-- PULL listing view, listing count, gms
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- listing count, gms
SELECT
is_digital,
is_supplies,
is_vintage,
is_handmade,
has_production_partner,
current_backend_labels,
current_frontend_handmade,
current_frontend_vintage,
is_nature_item,
is_giftbox,
is_pod,
variant_a_label,
CASE WHEN variant_a_label = "seller designed" THEN variant_a_label
     WHEN variant_a_label = "handmade" THEN variant_a_label
     WHEN variant_a_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_a_label IN ("not assigned","no label someone else") THEN "no label"
     END AS variant_a_main_label,
variant_b_label,
CASE WHEN variant_b_label = "seller designed" THEN variant_b_label
     WHEN variant_b_label = "handmade" THEN variant_b_label
     WHEN variant_b_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_b_label IN ("not assigned","no label someone else","no label pod") THEN "no label"
     END AS variant_b_main_label,
COUNT(listing_id) AS listing_ct,
SUM(past_year_gms) AS past_year_gms
FROM `etsy-data-warehouse-dev.nlao.him_joined_table`
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;

-- listing_view
SELECT
is_digital,
is_supplies,
is_vintage,
is_handmade,
has_production_partner,
current_backend_labels,
current_frontend_handmade,
current_frontend_vintage,
is_nature_item,
is_giftbox,
is_pod,
variant_a_label,
CASE WHEN variant_a_label = "seller designed" THEN variant_a_label
     WHEN variant_a_label = "handmade" THEN variant_a_label
     WHEN variant_a_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_a_label IN ("not assigned","no label someone else") THEN "no label"
     END AS variant_a_main_label,
variant_b_label,
CASE WHEN variant_b_label = "seller designed" THEN variant_b_label
     WHEN variant_b_label = "handmade" THEN variant_b_label
     WHEN variant_b_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_b_label IN ("not assigned","no label someone else","no label pod") THEN "no label"
     END AS variant_b_main_label,
COUNT(*) AS listing_view_ct,
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_joined_table` j
  USING (listing_id)
WHERE _date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;

-- impacted = any change
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_impact_reason` AS ( 
WITH seller_join AS (
SELECT
b.*,
CASE WHEN variant_a_label = "seller designed" THEN variant_a_label
     WHEN variant_a_label = "handmade" THEN variant_a_label
     WHEN variant_a_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_a_label IN ("not assigned","no label someone else") THEN "no label"
     END AS variant_a_main_label,
CASE WHEN variant_b_label = "seller designed" THEN variant_b_label
     WHEN variant_b_label = "handmade" THEN variant_b_label
     WHEN variant_b_label IN ("seller sourced and curated: nature item","seller sourced and curated: giftbox") THEN "seller sourced and curated"
     WHEN variant_b_label IN ("not assigned","no label someone else","no label pod") THEN "no label"
     END AS variant_b_main_label
-- COUNT(a.listing_id) OVER (PARTITION BY a.user_id) AS seller_own_active_listings
FROM `etsy-data-warehouse-dev.nlao.him_joined_table` b
)
SELECT 
*,
CASE WHEN variant_a_main_label = "seller designed" 
      OR (variant_a_main_label = "seller sourced and curated" AND current_frontend_handmade = 0)
    THEN 1 ELSE 0 END AS va_add_new_label,
CASE WHEN (variant_a_main_label = "handmade" AND current_frontend_handmade = 1) 
      OR (variant_a_main_label = "no label" AND current_frontend_handmade = 0) 
    THEN 1 ELSE 0 END AS va_not_impacted,
CASE WHEN variant_a_main_label = "no label" 
      AND current_frontend_handmade = 1 
    THEN 1 ELSE 0 END AS va_remove_handmade_label,
CASE WHEN variant_a_main_label = "seller sourced and curated" 
      AND current_frontend_handmade = 1 
    THEN 1 ELSE 0 END AS va_change_to_new_label,
CASE WHEN variant_b_main_label = "seller designed" 
      OR (variant_b_main_label = "seller sourced and curated" AND current_frontend_handmade = 0) 
    THEN 1 ELSE 0 END AS vb_add_new_label,
CASE WHEN (variant_b_main_label = "handmade" AND current_frontend_handmade = 1) 
      OR (variant_b_main_label = "no label" AND current_frontend_handmade = 0) 
    THEN 1 ELSE 0 END AS vb_not_impacted,
CASE WHEN variant_b_main_label = "no label" 
      AND current_frontend_handmade = 1 
    THEN 1 ELSE 0 END AS vb_remove_handmade_label,
CASE WHEN variant_b_main_label = "seller sourced and curated" 
      AND current_frontend_handmade = 1 
    THEN 1 ELSE 0 END AS vb_change_to_new_label,
FROM seller_join
)
;


-- one or more impacted
WITH impact_listing_ct AS (
SELECT
l.user_id,
s.seller_tier,
SUM(va_add_new_label) AS va_add_new_label_listing,
SUM(va_not_impacted) AS va_no_impact_listing,
SUM(va_remove_handmade_label) AS va_remove_handmade_label_listing,
SUM(va_change_to_new_label) AS va_change_to_new_label_listing,
SUM(vb_add_new_label) AS vb_add_new_label_listing,
SUM(vb_not_impacted) AS vb_no_impact_listing,
SUM(vb_remove_handmade_label) AS vb_remove_handmade_label_listing,
SUM(vb_change_to_new_label) AS vb_change_to_new_label_listing,
COUNT(*) AS total_listing_ct
FROM `etsy-data-warehouse-dev.nlao.him_impact_reason` h
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` l
  ON h.listing_id = l.listing_id
JOIN `etsy-data-warehouse-prod.rollups.seller_tier` s
  ON s.user_id = l.user_id AND s.date = CURRENT_DATE - 1
GROUP BY 1,2
)
SELECT
CASE WHEN va_no_impact_listing = total_listing_ct THEN "1.no impact"
     WHEN va_add_new_label_listing >0 AND va_remove_handmade_label_listing = 0 AND va_change_to_new_label_listing = 0 THEN "2.add_new_label_listing ONLY"
     WHEN va_add_new_label_listing =0 AND va_remove_handmade_label_listing > 0 AND va_change_to_new_label_listing = 0 THEN "3.remove_handmade_label_listing ONLY"
     WHEN va_add_new_label_listing =0 AND va_remove_handmade_label_listing = 0 AND va_change_to_new_label_listing > 0 THEN "4.change_to_new_label_listing ONLY"
     WHEN va_add_new_label_listing >0 AND va_remove_handmade_label_listing > 0 AND va_change_to_new_label_listing = 0 THEN "5.add and remove label"
     WHEN va_add_new_label_listing =0 AND va_remove_handmade_label_listing > 0 AND va_change_to_new_label_listing > 0 THEN "6.remove and change label"
     WHEN va_add_new_label_listing >0 AND va_remove_handmade_label_listing = 0 AND va_change_to_new_label_listing > 0 THEN "7.add and change label"
     WHEN va_add_new_label_listing >0 AND va_remove_handmade_label_listing > 0 AND va_change_to_new_label_listing > 0 THEN "8.all three changes"
END AS va_impact_grouping,
CASE WHEN vb_no_impact_listing = total_listing_ct THEN "1.no impact"
     WHEN vb_add_new_label_listing >0 AND vb_remove_handmade_label_listing = 0 AND vb_change_to_new_label_listing = 0 THEN "2.add_new_label_listing ONLY"
     WHEN vb_add_new_label_listing =0 AND vb_remove_handmade_label_listing > 0 AND vb_change_to_new_label_listing = 0 THEN "3.remove_handmade_label_listing ONLY"
     WHEN vb_add_new_label_listing =0 AND vb_remove_handmade_label_listing = 0 AND vb_change_to_new_label_listing > 0 THEN "4.change_to_new_label_listing ONLY"
     WHEN vb_add_new_label_listing >0 AND vb_remove_handmade_label_listing > 0 AND vb_change_to_new_label_listing = 0 THEN "5.add and remove label"
     WHEN vb_add_new_label_listing =0 AND vb_remove_handmade_label_listing > 0 AND vb_change_to_new_label_listing > 0 THEN "6.remove and change label"
     WHEN vb_add_new_label_listing >0 AND vb_remove_handmade_label_listing = 0 AND vb_change_to_new_label_listing > 0 THEN "7.add and change label"
     WHEN vb_add_new_label_listing >0 AND vb_remove_handmade_label_listing > 0 AND vb_change_to_new_label_listing > 0 THEN "8.all three changes"
END AS vb_impact_grouping,
seller_tier,
COUNT(user_id) AS user_ct
FROM impact_listing_ct
GROUP BY 1,2,3
;

-- 50% or more impacted
WITH impact_listing_ct AS (
SELECT
l.user_id,
s.seller_tier,
SUM(va_add_new_label) AS va_add_new_label_listing,
SUM(va_not_impacted) AS va_no_impact_listing,
SUM(va_remove_handmade_label) AS va_remove_handmade_label_listing,
SUM(va_change_to_new_label) AS va_change_to_new_label_listing,
SUM(vb_add_new_label) AS vb_add_new_label_listing,
SUM(vb_not_impacted) AS vb_no_impact_listing,
SUM(vb_remove_handmade_label) AS vb_remove_handmade_label_listing,
SUM(vb_change_to_new_label) AS vb_change_to_new_label_listing,
COUNT(*) AS total_listing_ct
FROM `etsy-data-warehouse-dev.nlao.him_impact_reason` h
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` l
  ON h.listing_id = l.listing_id
JOIN `etsy-data-warehouse-prod.rollups.seller_tier` s
  ON s.user_id = l.user_id AND s.date = CURRENT_DATE - 1
GROUP BY 1,2
)
SELECT
CASE WHEN va_no_impact_listing = total_listing_ct THEN "1.no impact"
     WHEN va_add_new_label_listing/total_listing_ct >=0.5 
        AND va_remove_handmade_label_listing = 0 
        AND va_change_to_new_label_listing = 0 
     THEN "2.add_new_label_listing ONLY"
     WHEN va_add_new_label_listing =0 
        AND va_remove_handmade_label_listing/total_listing_ct >= 0.5 
        AND va_change_to_new_label_listing = 0 
     THEN "3.remove_handmade_label_listing ONLY"
     WHEN va_add_new_label_listing =0 
        AND va_remove_handmade_label_listing = 0 
        AND va_change_to_new_label_listing/total_listing_ct >= 0.5 
     THEN "4.change_to_new_label_listing ONLY"
     WHEN va_add_new_label_listing/total_listing_ct >=0.5 
        AND va_remove_handmade_label_listing > 0 
        AND va_change_to_new_label_listing = 0 
     THEN "5.add and remove label"
     WHEN va_add_new_label_listing =0 
        AND va_remove_handmade_label_listing/total_listing_ct >=0.5
        AND va_change_to_new_label_listing/total_listing_ct >= 0.5
     THEN "6.remove and change label"
     WHEN va_add_new_label_listing/total_listing_ct >=0.5 
        AND va_remove_handmade_label_listing = 0 
        AND va_change_to_new_label_listing/total_listing_ct >=0.5 
     THEN "7.add and change label"
     WHEN va_add_new_label_listing/total_listing_ct >=0.5
        AND va_remove_handmade_label_listing/total_listing_ct >= 0.5
        AND va_change_to_new_label_listing/total_listing_ct >= 0.5 
     THEN "8.all three changes"
     ELSE "1.no impact"
     END AS va_impact_grouping,
CASE WHEN vb_no_impact_listing = total_listing_ct 
     THEN "1.no impact"
     WHEN vb_add_new_label_listing/total_listing_ct >=0.5 
        AND vb_remove_handmade_label_listing = 0 
        AND vb_change_to_new_label_listing = 0 
     THEN "2.add_new_label_listing ONLY"
     WHEN vb_add_new_label_listing =0 
        AND vb_remove_handmade_label_listing/total_listing_ct >= 0.5 
        AND vb_change_to_new_label_listing = 0 
     THEN "3.remove_handmade_label_listing ONLY"
     WHEN vb_add_new_label_listing =0 
        AND vb_remove_handmade_label_listing = 0 
        AND vb_change_to_new_label_listing/total_listing_ct >= 0.5 
     THEN "4.change_to_new_label_listing ONLY"
     WHEN vb_add_new_label_listing/total_listing_ct >=0.5 
        AND vb_remove_handmade_label_listing > 0 
        AND vb_change_to_new_label_listing = 0 
     THEN "5.add and remove label"
     WHEN vb_add_new_label_listing =0 
        AND vb_remove_handmade_label_listing/total_listing_ct >=0.5
        AND vb_change_to_new_label_listing/total_listing_ct >= 0.5
     THEN "6.remove and change label"
     WHEN vb_add_new_label_listing/total_listing_ct >=0.5 
        AND vb_remove_handmade_label_listing = 0 
        AND vb_change_to_new_label_listing/total_listing_ct >=0.5 
     THEN "7.add and change label"
     WHEN vb_add_new_label_listing/total_listing_ct >=0.5
        AND vb_remove_handmade_label_listing/total_listing_ct >= 0.5
        AND vb_change_to_new_label_listing/total_listing_ct >= 0.5 
     THEN "8.all three changes"
     ELSE "1.no impact"
     END AS vb_impact_grouping,
seller_tier,
COUNT(user_id) AS user_ct
FROM impact_listing_ct
GROUP BY 1,2,3
;



