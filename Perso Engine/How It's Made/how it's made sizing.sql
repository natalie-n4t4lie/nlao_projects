CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` AS (
WITH production_partner_listings AS (
SELECT DISTINCT 
  sppla.listing_id,
  1 AS value
FROM `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  ON sppla.production_partner_id = spp.production_partner_id
)
, labels AS (
SELECT
is_digital,
COALESCE(CAST(b.value AS INT64),0) AS is_supplies,
COALESCE(CAST(c.value AS INT64),0) AS is_vintage,
COALESCE(CAST(d.value AS INT64),0) AS is_handmade,
COALESCE(e.value ,0) AS has_production_partner,
a.listing_id,
a.past_year_gms,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN (SELECT listing_id,concept.value FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments` WHERE concept.type = "is_supplies") b ON a.listing_id = b.listing_id
LEFT JOIN (SELECT listing_id,concept.value FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments` WHERE concept.type = "is_vintage") c ON a.listing_id = c.listing_id
LEFT JOIN (SELECT listing_id,concept.value FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments` WHERE concept.type = "is_handmade") d ON a.listing_id = d.listing_id
LEFT JOIN production_partner_listings e ON a.listing_id = e.listing_id
)
,backend_labels AS (
SELECT
CASE WHEN is_digital>=1 THEN 1 ELSE 0 END AS is_digital,
is_supplies,
is_vintage,
is_handmade,
has_production_partner,
CASE WHEN is_handmade = 0 AND is_supplies = 0 AND is_vintage = 0 AND has_production_partner = 1 THEN "Handmade with production assistance"
     WHEN is_handmade = 1 AND is_supplies = 0 AND is_vintage = 0 THEN "Handmade"
     WHEN is_handmade = 1 AND is_supplies = 1 AND is_vintage = 0 THEN "Handmade Supply"
     WHEN is_handmade = 0 AND is_supplies = 1 AND is_vintage = 0 THEN "Supply"
     WHEN is_handmade = 0 AND is_supplies = 0 AND is_vintage = 1 AND has_production_partner = 0 THEN "Vintage"
     WHEN is_handmade = 0 AND is_supplies = 1 AND is_vintage = 1 AND has_production_partner = 0 THEN "Vintage Supply"
     WHEN is_handmade = 1 AND is_supplies = 0 AND is_vintage = 1 AND has_production_partner = 0 THEN "Handmade and Vintage"
     WHEN is_handmade = 1 AND is_supplies = 1 AND is_vintage = 1 AND has_production_partner = 0 THEN "Handmade, Vintage Supply"
     ELSE "N/A" END AS back_end_labels,
listing_id,
past_year_gms
FROM labels
)
SELECT
*,
CASE WHEN is_digital = 0 AND (is_handmade = 1 OR has_production_partner = 1) THEN 1 ELSE 0 END AS show_handmade,
CASE WHEN is_vintage = 1 THEN 1 ELSE 0 END AS show_vintage,
FROM backend_labels
);


--listing ct, gms
SELECT
is_digital,
is_supplies,
is_vintage,
is_handmade,
has_production_partner,
back_end_labels,
show_handmade,
show_vintage,
COUNT(DISTINCT listing_id) AS listing_ct,
SUM(past_year_gms) AS past_year_gms
FROM `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels`
GROUP BY 1,2,3,4,5,6,7,8
;

-- listings, sellers, listing views, transactions, GMS
SELECT
COALESCE(back_end_labels,"N/A") AS back_end_labels,
COALESCE(is_digital,0) AS is_digital,
COALESCE(is_supplies,0) AS is_supplies,
COALESCE(is_vintage,0) AS is_vintage,
COALESCE(is_handmade,0) AS is_handmade,
COALESCE(has_production_partner,0) AS has_production_partner,
COALESCE(show_handmade,0) AS show_handmade,
COALESCE(show_vintage,0) AS show_vintage,
COUNT(l.listing_id) AS listing_view_ct
FROM `etsy-data-warehouse-prod.analytics.listing_views` l
LEFT JOIN  `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` a
  ON l.listing_id = a.listing_id
WHERE _date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8
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

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_labels` AS (
SELECT
CASE WHEN b.label IS NOT NULL THEN 1 ELSE 0 END AS is_nature_item,
CASE WHEN c.label IS NOT NULL THEN 1 ELSE 0 END AS is_giftbox,
CASE WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_pod_partner`) THEN "Has production partner, is POD"
     WHEN a.listing_id IN (select listing_id FROM `etsy-data-warehouse-dev.nlao.him_non_pod_partner`) THEN "Has production partner, not POD"
     ELSE "No production partner" END AS production_partner_type,
CASE WHEN a.is_digital>=1 THEN 1 ELSE 0 END AS is_digital,
a.listing_id,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_sourced_and_curated` b ON a.listing_id = b.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_seller_giftbox` c on a.listing_id = c.listing_id
)
;

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
b.production_partner_type,
CASE WHEN b.is_nature_item = 1 AND a.is_digital = 0 AND production_partner_type IN ("Has production partner, not POD","No production partner") THEN 1 ELSE 0 END AS proposed_nature_item,
CASE WHEN b.is_giftbox = 1 AND a.is_digital = 0 AND production_partner_type IN ("Has production partner, not POD","No production partner") THEN 1 ELSE 0 END AS proposed_gift_box,
CASE WHEN production_partner_type = "Has production partner, is POD" THEN 1 ELSE 0 END AS proposed_pod,
CASE WHEN a.is_digital = 1 THEN 1 ELSE 0 END AS proposed_digital,
CASE WHEN a.show_vintage = 1 THEN 1 ELSE 0 END AS proposed_vintage,
CASE WHEN a.show_handmade = 1 AND is_giftbox = 0 AND is_nature_item = 0 AND a.back_end_labels != "Handmade with production assistance" THEN 1 ELSE 0 END AS proposed_handmade,
CASE WHEN a.show_handmade = 1 AND is_giftbox = 0 AND is_nature_item = 0 AND a.back_end_labels = "Handmade with production assistance" THEN 1 ELSE 0 END AS proposed_remove_handmade,
COUNT(listing_id) AS active_listing_ct,
sum(past_year_gms) AS past_year_gms
FROM `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` a
JOIN `etsy-data-warehouse-dev.nlao.him_labels` b using (listing_id)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;



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
b.production_partner_type,
CASE WHEN b.is_nature_item = 1 AND a.is_digital = 0 AND production_partner_type IN ("Has production partner, not POD","No production partner") THEN 1 ELSE 0 END AS proposed_nature_item,
CASE WHEN b.is_giftbox = 1 AND a.is_digital = 0 AND production_partner_type IN ("Has production partner, not POD","No production partner") THEN 1 ELSE 0 END AS proposed_gift_box,
CASE WHEN production_partner_type = "Has production partner, is POD" THEN 1 ELSE 0 END AS proposed_pod,
CASE WHEN a.is_digital = 1 THEN 1 ELSE 0 END AS proposed_digital,
CASE WHEN a.show_vintage = 1 THEN 1 ELSE 0 END AS proposed_vintage,
CASE WHEN a.show_handmade = 1 AND is_giftbox = 0 AND is_nature_item = 0 AND a.back_end_labels != "Handmade with production assistance" THEN 1 ELSE 0 END AS proposed_handmade,
CASE WHEN a.show_handmade = 1 AND is_giftbox = 0 AND is_nature_item = 0 AND a.back_end_labels = "Handmade with production assistance" THEN 1 ELSE 0 END AS proposed_remove_handmade,
COUNT(l.listing_id) AS active_listing_ct,
FROM `etsy-data-warehouse-prod.analytics.listing_views` l
LEFT JOIN `etsy-data-warehouse-dev.nlao.current_handmade_vintage_labels` a
  ON a.listing_id = l.listing_id
LEFT JOIN `etsy-data-warehouse-dev.nlao.him_labels` b
  ON l.listing_id = b.listing_id
WHERE _date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

