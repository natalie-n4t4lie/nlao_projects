BEGIN

DECLARE START_DATE DATE default "2025-04-21"; 
DECLARE END_DATE DATE default "2025-04-22"; 

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.recs_visits` AS (

WITH visits AS (
 SELECT
    beacon.timestamp as event_timestamp,
    v.visit_id,
    v.beacon.guid as event_id,
    beacon.event_name as event_name,
    beacon.event_source as event_source,
    beacon.ref as ref,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "source") AS _source,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_ids") AS _listing_ids,
    CASE WHEN v.beacon.event_name = "recs_listing_set_metadata" THEN (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "module_placement") ELSE (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "placement") END AS _placement_beacon,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "page_guid") AS _page_guid,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "interaction_type") AS _interaction_type,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_set_key") AS _listing_set_key,
    sequence_number
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` v
  WHERE DATE(v._PARTITIONTIME) BETWEEN START_DATE AND END_DATE
  AND DATE(TIMESTAMP_SECONDS(CAST(beacon.event_timestamp / 1000 AS INT64))) BETWEEN START_DATE AND END_DATE
  AND ((v.beacon.event_name IN ('listing_set_delivered','listing_impression','listing_interaction','recs_listing_set_metadata') 
  AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "source") = 'recs'
   ) OR (v.beacon.event_name = "recs_listing_set_metadata"))
)
SELECT
  v.event_timestamp,
  v.visit_id,
  visit.browser_id,
  visit.platform,
  v.event_name,
  v.event_source,
  v.ref,
  v._source,
  v._listing_ids,
  v._placement_beacon,
  v._page_guid,
  v._interaction_type,
  v._listing_set_key
  FROM visits v
  INNER JOIN `etsy-data-warehouse-prod.weblog.recent_visits` visit on v.visit_id = visit.visit_id
  WHERE visit._date BETWEEN START_DATE AND END_DATE
  AND visit.platform in ('desktop', 'mobile_web','boe')
)
;

END;


BEGIN

DECLARE START_DATE DATE default "2025-04-21"; 
DECLARE END_DATE DATE default "2025-04-22"; 

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.recs_legacy_visits` AS (
 SELECT DISTINCT
    beacon.timestamp as event_timestamp,
    v.visit_id,
    v.beacon.guid as event_id,
    beacon.event_name as event_name,
    beacon.event_source as event_source,
    beacon.ref as ref,
    platform,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "module_placement") AS _placement_beacon,
    sequence_number
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` v
  INNER JOIN `etsy-data-warehouse-prod.weblog.recent_visits` visit on v.visit_id = visit.visit_id
  WHERE visit._date BETWEEN START_DATE AND END_DATE
  AND visit.platform in ('desktop', 'mobile_web','boe')
  AND DATE(v._PARTITIONTIME) BETWEEN START_DATE AND END_DATE
  AND DATE(TIMESTAMP_SECONDS(CAST(beacon.event_timestamp / 1000 AS INT64))) BETWEEN START_DATE AND END_DATE
  AND v.beacon.event_name IN ('recommendations_module_delivered','recommendations_module_seen') 
  AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_ids") IS NOT NULL
)
;

END;


-- STEP 1
SELECT 
DATE(TIMESTAMP_SECONDS(CAST(event_timestamp / 1000 AS INT64))) as event_date,
platform,
_placement_beacon,
  SUM(CASE WHEN event_name = 'listing_set_delivered'THEN 1 ELSE 0 END) AS total_recs_unified_delivered,
  SUM(CASE WHEN event_name = 'recs_listing_set_metadata'THEN 1 ELSE 0 END) AS total_recs_unified_meta,
  SUM(CASE WHEN event_name = 'listing_impression' THEN 1 ELSE 0 END) AS total_recs_unified_impressions,
  SUM(CASE WHEN event_name = 'listing_interaction' and _interaction_type IN ('click','tap') THEN 1 ELSE 0 END) AS total_recs_unified_clicks,  
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE DATE(TIMESTAMP_SECONDS(CAST(event_timestamp / 1000 AS INT64))) = '2025-04-22'
GROUP BY ALL
ORDER BY 2,4 DESC
;


-- STEP 3
WITH unified_deliveries AS (
SELECT * FROM `etsy-data-warehouse-dev.nlao.recs_visits` 
WHERE event_name = 'listing_set_delivered'
AND _placement_beacon IN (
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks",
    "boe_homescreen_recs_placeholder_module",
    "boe_homescreen_feed",
    "boe_homescreen_recs_placeholder_module_3",
    "boe_homescreen_post_purchase_people_also_bought",
    "listing_side",
    "home_opfy",
    "internal_bot",
    "external_top",
    "pla_top",
    "home_rv",
    "external_bot",
    "home_signed_out_opfy",
    "lp_free_shipping_bundle",
    "home_rf",
    "home_popular_right_now",
    "pla_bot",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_item_favoriting_quiz",
    "post_add_to_cart_tipper_recs",
    "home_more_from_this_shop",
    "pla_top",
    "listing_side",
    "external_top",
    "internal_bot",
    "lp_recently_viewed",
    "home_rv",
    "external_bot",
    "home_rf",
    "pla_bot",
    "home_opfy",
    "home_signed_out_opfy",
    "home_popular_right_now",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_more_from_this_shop",
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks"
    )
),
unified_meta AS (
SELECT * FROM `etsy-data-warehouse-dev.nlao.recs_visits` 
WHERE event_name = 'recs_listing_set_metadata'
AND _placement_beacon IN (
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks",
    "boe_homescreen_recs_placeholder_module",
    "boe_homescreen_feed",
    "boe_homescreen_recs_placeholder_module_3",
    "boe_homescreen_post_purchase_people_also_bought",
    "listing_side",
    "home_opfy",
    "internal_bot",
    "external_top",
    "pla_top",
    "home_rv",
    "external_bot",
    "home_signed_out_opfy",
    "lp_free_shipping_bundle",
    "home_rf",
    "home_popular_right_now",
    "pla_bot",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_item_favoriting_quiz",
    "post_add_to_cart_tipper_recs",
    "home_more_from_this_shop",
    "pla_top",
    "listing_side",
    "external_top",
    "internal_bot",
    "lp_recently_viewed",
    "home_rv",
    "external_bot",
    "home_rf",
    "pla_bot",
    "home_opfy",
    "home_signed_out_opfy",
    "home_popular_right_now",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_more_from_this_shop",
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks"
    )
),
legacy_deliveries AS (
SELECT * FROM `etsy-data-warehouse-dev.nlao.recs_legacy_visits` 
WHERE event_name = 'recommendations_module_delivered'
AND _placement_beacon IN (
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks",
    "boe_homescreen_recs_placeholder_module",
    "boe_homescreen_feed",
    "boe_homescreen_recs_placeholder_module_3",
    "boe_homescreen_post_purchase_people_also_bought",
    "listing_side",
    "home_opfy",
    "internal_bot",
    "external_top",
    "pla_top",
    "home_rv",
    "external_bot",
    "home_signed_out_opfy",
    "lp_free_shipping_bundle",
    "home_rf",
    "home_popular_right_now",
    "pla_bot",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_item_favoriting_quiz",
    "post_add_to_cart_tipper_recs",
    "home_more_from_this_shop",
    "pla_top",
    "listing_side",
    "external_top",
    "internal_bot",
    "lp_recently_viewed",
    "home_rv",
    "external_bot",
    "home_rf",
    "pla_bot",
    "home_opfy",
    "home_signed_out_opfy",
    "home_popular_right_now",
    "hp_recent_activity_hub",
    "home_people_also_bought",
    "home_more_from_this_shop",
    "boe_homescreen_evergreen_interests",
    "boe_homescreen_our_picks"
    )
)
SELECT
COALESCE(u._placement_beacon,um._placement_beacon,l._placement_beacon) AS _placement_beacon,
COALESCE(u.platform,um.platform,l.platform) AS platform,
COUNT(*) AS total_deliveries_ct,
COUNT(CASE WHEN u._placement_beacon IS NOT NULL AND um._placement_beacon IS NOT NULL AND l._placement_beacon IS NOT NULL THEN u.visit_id END) AS overlap_deliveries,
COUNT(CASE WHEN u._placement_beacon IS NOT NULL AND um._placement_beacon IS NULL AND l._placement_beacon IS NULL THEN u.visit_id END) AS unified_deliveries_only_ct,
COUNT(CASE WHEN u._placement_beacon IS NULL AND um._placement_beacon IS NOT NULL AND l._placement_beacon IS NULL THEN um.visit_id END) AS unified_meta_only_ct,
COUNT(CASE WHEN u._placement_beacon IS NULL AND um._placement_beacon IS NULL AND l._placement_beacon IS NOT NULL THEN l.visit_id END) AS legacy_deliveries_only_ct,
COUNT(CASE WHEN u._placement_beacon IS NOT NULL AND um._placement_beacon IS NOT NULL AND l._placement_beacon IS NULL THEN u.visit_id END) AS unified_delivies_meta_overlap_ct,
COUNT(CASE WHEN u._placement_beacon IS NOT NULL AND um._placement_beacon IS NULL AND l._placement_beacon IS NOT NULL THEN u.visit_id END) AS unified_delivies_legacy_overlap_ct,
COUNT(CASE WHEN u._placement_beacon IS NULL AND um._placement_beacon IS NOT NULL AND l._placement_beacon IS NOT NULL THEN l.visit_id END) AS unified_meta_legacy_overlap_ct,
FROM unified_deliveries u
FULL OUTER JOIN legacy_deliveries l
ON u.visit_id = l.visit_id AND u._placement_beacon=l._placement_beacon
FULL OUTER JOIN unified_meta um
ON u.visit_id = um.visit_id AND u._placement_beacon=um._placement_beacon
GROUP BY ALL
;


-- STEP 4: SEEN EVENT VALIDATION

-- legacy
SELECT 
  platform,
  module_placement,
  SUM(CASE WHEN seen = 1 THEN 1 ELSE 0 END) AS legacy_seen_ct
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
WHERE _date BETWEEN '2025-04-21'AND '2025-04-22'
  AND seen = 1
  AND ((platform = 'boe' AND  _placement_beacon IN ("boe_homescreen_evergreen_interests",
                                            "boe_homescreen_our_picks",
                                            "boe_homescreen_recs_placeholder_module",
                                            "boe_homescreen_feed",
                                            "boe_homescreen_recs_placeholder_module_3",
                                            "boe_homescreen_post_purchase_people_also_bought")
)
OR
(platform = 'dekstop' AND  _placement_beacon IN ("listing_side",
                                              "home_opfy",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "home_rv",
                                              "external_bot",
                                              "home_signed_out_opfy",
                                              "lp_free_shipping_bundle",
                                              "home_rf",
                                              "home_popular_right_now",
                                              "pla_bot",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_item_favoriting_quiz",
                                              "post_add_to_cart_tipper_recs",
                                              "home_more_from_this_shop")
)
OR 
(platform = 'mobile_web' AND  _placement_beacon IN ("pla_top",
                                              "listing_side",
                                              "external_top",
                                              "internal_bot",
                                              "lp_recently_viewed",
                                              "home_rv",
                                              "external_bot",
                                              "home_rf",
                                              "pla_bot",
                                              "home_opfy",
                                              "home_signed_out_opfy",
                                              "home_popular_right_now",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_more_from_this_shop",
                                              "boe_homescreen_evergreen_interests",
                                              "boe_homescreen_our_picks",
                                              "boe_homescreen_recs_placeholder_module")
))
GROUP BY ALL
;

-- unified
SELECT 
  platform,
  _placement_beacon,
  COUNT(*) AS unified_seen_ct
FROM `etsy-data-warehouse-dev.nlao.recs_visits` 
WHERE event_name = 'listing_impression'
  AND ((platform = 'boe' AND  _placement_beacon IN ("boe_homescreen_evergreen_interests",
                                            "boe_homescreen_our_picks",
                                            "boe_homescreen_recs_placeholder_module",
                                            "boe_homescreen_feed",
                                            "boe_homescreen_recs_placeholder_module_3",
                                            "boe_homescreen_post_purchase_people_also_bought")
)
OR
(platform = 'dekstop' AND  _placement_beacon IN ("listing_side",
                                              "home_opfy",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "home_rv",
                                              "external_bot",
                                              "home_signed_out_opfy",
                                              "lp_free_shipping_bundle",
                                              "home_rf",
                                              "home_popular_right_now",
                                              "pla_bot",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_item_favoriting_quiz",
                                              "post_add_to_cart_tipper_recs",
                                              "home_more_from_this_shop")
)
OR 
(platform = 'mobile_web' AND  _placement_beacon IN ("pla_top",
                                              "listing_side",
                                              "external_top",
                                              "internal_bot",
                                              "lp_recently_viewed",
                                              "home_rv",
                                              "external_bot",
                                              "home_rf",
                                              "pla_bot",
                                              "home_opfy",
                                              "home_signed_out_opfy",
                                              "home_popular_right_now",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_more_from_this_shop",
                                              "boe_homescreen_evergreen_interests",
                                              "boe_homescreen_our_picks",
                                              "boe_homescreen_recs_placeholder_module")
)
      )
GROUP BY ALL
;


-- STEP 5: CLICK EVENT VALIDATION

-- legacy
SELECT 
  platform,
  module_placement,
  SUM(CASE WHEN clicked = 1 THEN 1 ELSE 0 END) AS legacy_click_ct
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
WHERE _date BETWEEN '2025-04-21'AND '2025-04-22'
  AND (platform = 'boe' AND  _placement_beacon IN ("boe_homescreen_evergreen_interests",
                                            "boe_homescreen_our_picks",
                                            "boe_homescreen_recs_placeholder_module",
                                            "boe_homescreen_feed",
                                            "boe_homescreen_recs_placeholder_module_3",
                                            "boe_homescreen_post_purchase_people_also_bought")
)
OR
(platform = 'dekstop' AND  _placement_beacon IN ("listing_side",
                                              "home_opfy",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "home_rv",
                                              "external_bot",
                                              "home_signed_out_opfy",
                                              "lp_free_shipping_bundle",
                                              "home_rf",
                                              "home_popular_right_now",
                                              "pla_bot",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_item_favoriting_quiz",
                                              "post_add_to_cart_tipper_recs",
                                              "home_more_from_this_shop")
)
OR 
(platform = 'mobile_web' AND  _placement_beacon IN ("pla_top",
                                              "listing_side",
                                              "external_top",
                                              "internal_bot",
                                              "lp_recently_viewed",
                                              "home_rv",
                                              "external_bot",
                                              "home_rf",
                                              "pla_bot",
                                              "home_opfy",
                                              "home_signed_out_opfy",
                                              "home_popular_right_now",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_more_from_this_shop",
                                              "boe_homescreen_evergreen_interests",
                                              "boe_homescreen_our_picks",
                                              "boe_homescreen_recs_placeholder_module")
)
GROUP BY ALL
;

-- unified
SELECT 
  platform,
  _placement_beacon,
  COUNT(*) AS unified_click_ct
FROM `etsy-data-warehouse-dev.nlao.recs_visits` 
WHERE event_name = 'listing_interaction' 
  AND _interaction_type IN ('click','tap')
  AND ((platform = 'boe' AND  _placement_beacon IN ("boe_homescreen_evergreen_interests",
                                            "boe_homescreen_our_picks",
                                            "boe_homescreen_recs_placeholder_module",
                                            "boe_homescreen_feed",
                                            "boe_homescreen_recs_placeholder_module_3",
                                            "boe_homescreen_post_purchase_people_also_bought")
)
OR
(platform = 'dekstop' AND  _placement_beacon IN ("listing_side",
                                              "home_opfy",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "home_rv",
                                              "external_bot",
                                              "home_signed_out_opfy",
                                              "lp_free_shipping_bundle",
                                              "home_rf",
                                              "home_popular_right_now",
                                              "pla_bot",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_item_favoriting_quiz",
                                              "post_add_to_cart_tipper_recs",
                                              "home_more_from_this_shop")
)
OR 
(platform = 'mobile_web' AND  _placement_beacon IN ("pla_top",
                                              "listing_side",
                                              "external_top",
                                              "internal_bot",
                                              "lp_recently_viewed",
                                              "home_rv",
                                              "external_bot",
                                              "home_rf",
                                              "pla_bot",
                                              "home_opfy",
                                              "home_signed_out_opfy",
                                              "home_popular_right_now",
                                              "hp_recent_activity_hub",
                                              "home_people_also_bought",
                                              "home_more_from_this_shop",
                                              "boe_homescreen_evergreen_interests",
                                              "boe_homescreen_our_picks",
                                              "boe_homescreen_recs_placeholder_module")
))
GROUP BY ALL
;
