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
WHERE top_cateogry IN ("craft_supplies_and_tools")
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
WHERE top_cateogry IN ("paper_and_party_supplies")
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
create or replace table `etsy-data-warehouse-dev.nlao.pod_active_listings`
as (
with sppla_mapping as (
select
  distinct 
  spp.shop_id,
  sppla.listing_id,
  spp.business_name,
  spp.descriptive_title,
  spp.about_production_partner,
  1 as is_pod_partner_listing
from `etsy-data-warehouse-prod.etsy_shard.shop_production_partner_listing_association` sppla
join
  `etsy-data-warehouse-prod.etsy_shard.shop_production_partner` spp
  on
  sppla.production_partner_id = spp.production_partner_id
where
  lower(spp.business_name) like "%print%"
  or
  lower(spp.descriptive_title) like "%print%"
  or lower(spp.about_production_partner) like "%print%"
),
api_users as (
   select user_id,
      api_key_id,
      count(*) as recs
   from `etsy-data-warehouse-prod.etsy_shard.user_api_keys`
   where active = 1
   group by 1,2
   order by 1,2
   ),
apps as (
   select distinct
   t.user_id,
   a.name,
   a.application_id,
   a.description,
   1 as is_pod_api_shop
   from api_users t 
   join `etsy-data-warehouse-prod.etsy_index.api_keys_index` k 
      on t.api_key_id = k.api_key_id
   join `etsy-data-warehouse-prod.etsy_shard.application_data` a 
      on k.app_id = a.application_id
   join `etsy-data-warehouse-prod.etsy_index.application_index` i 
      on a.application_id = i.application_id
   where a.name not in ("Etsy for iPhone","Etsy for Android","Sell on Etsy for Android","Butter SOE (iOS)","Butter SOE (Android)")
      and i.approved = 1 
and (lower(a.name) like "%printful%" or lower(a.name) like "%printify%"
or lower(a.name) like "%gooten%"
or lower(a.name) like "%inkthreadable%")
      AND app_id IN (692758447517,1048119756481,182574138636,194131186944,826789741498,583596392566,404172100923,1021341745869,293514109927,908698012787,403922718747,513342174141,16100456054,1164547402740,914535233482,1153967920197,1112050276391,412564431162,1020017555556)
      --and i.state = 1
)
select
l.*,
s.seller_tier_new,
s.shop_name,
s.active_seller_status,
s.sws_status,
s.past_year_gms as shop_past_year_gms,
p.is_pod_partner_listing,
p.business_name as partner_business_name,
p.descriptive_title as partner_descriptive_title,
p.about_production_partner as about_production_partner,
a.is_pod_api_shop,
a.name as api_name,
a.application_id as application_id,
a.description as api_description
from `etsy-data-warehouse-prod.rollups.active_listing_basics` l
join `etsy-data-warehouse-prod.rollups.seller_basics` s using (shop_id)
left join sppla_mapping p on l.listing_id = p.listing_id
left join apps a on s.user_id = a.user_id);

-- # POD ACTIVE LISTINGS
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
WITH def AS (
SELECT
"Seller designed POD (Partner string match)" AS label,
listing_id
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
WHERE is_pod_partner_listing = 1
ORDER BY RAND()
LIMIT 20
;

--# Seller designed POD (API match)
-- listing count, gms, and seller count
WITH def AS (
SELECT
DISTINCT listing_id
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
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
FROM `etsy-data-warehouse-dev.nlao.pod_active_listings`
WHERE is_pod_api_shop = 1
ORDER BY RAND()
LIMIT 20
;
