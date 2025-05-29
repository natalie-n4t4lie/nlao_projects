BEGIN

DECLARE START_DATE DATE default "2025-05-07"; 
DECLARE END_DATE DATE default "2025-05-08"; 

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.recs_visits` AS (

WITH visits AS (
 SELECT DISTINCT
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

-- Validation 1: unified delivered & unified meta
WITH delivered AS (
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'listing_set_delivered'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
)
, meta AS (
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'recs_listing_set_metadata'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
)
SELECT
COALESCE(d.platform,m.platform) AS platform,
COALESCE(d._placement_beacon,m._placement_beacon) AS _placement_beacon,
CASE WHEN d.visit_id IS NOT NULL THEN 1 ELSE 0 END AS unified_delivered,
CASE WHEN m.visit_id IS NOT NULL THEN 1 ELSE 0 END AS unified_meta,
COUNT(*) AS delivered_ct,
FROM delivered d
FULL OUTER JOIN meta m
ON d._listing_set_key = m._listing_set_key AND d.visit_id = m.visit_id
GROUP BY ALL
;

-- validation 2: unified event click through rate
SELECT
platform,
_placement_beacon,
COUNT(CASE WHEN event_name = 'listing_impression' THEN visit_id ELSE NULL END) AS impression_ct,
COUNT(CASE WHEN event_name = 'listing_interaction' THEN visit_id ELSE NULL END) AS click_ct,
FROM `etsy-data-warehouse-dev.nlao.recs_visits` 
WHERE platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
)
GROUP BY ALL 
ORDER BY 1,3 DESC
;

-- validation 3: unified delivered vs legacy delivered
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ud_um_ld_event_ct` AS (
WITH delivered AS (
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'listing_set_delivered'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
)
, meta AS (
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'recs_listing_set_metadata'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
)
, unified_aggregate_deliveries AS (
SELECT
d.platform,
d._placement_beacon,
d.visit_id,
COUNT(*) AS ct
FROM delivered d
JOIN meta m
ON d._listing_set_key = m._listing_set_key AND d.visit_id = m.visit_id
GROUP BY ALL
)
,legacy_deliveries AS (
SELECT 
platform,
_placement_beacon,
visit_id,
COUNT(*) AS ct
FROM `etsy-data-warehouse-dev.nlao.recs_legacy_visits` 
WHERE event_name = 'recommendations_module_delivered'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
)
GROUP BY ALL
)
SELECT
COALESCE(u._placement_beacon,l._placement_beacon) AS _placement_beacon,
COALESCE(u.platform,l.platform) AS platform,
COALESCE(u.visit_id,l.visit_id) AS visit_id,
u.ct AS unified_delivered_ct,
l.ct AS legacy_delivered_ct
FROM unified_aggregate_deliveries u
FULL OUTER JOIN legacy_deliveries l
ON u.visit_id = l.visit_id AND u._placement_beacon=l._placement_beacon AND u.platform = l.platform
);

-- comparing unified delivered & legacy per visit
SELECT
platform,
_placement_beacon,
CASE WHEN unified_delivered_ct = legacy_delivered_ct THEN "1. matchup"
     WHEN unified_delivered_ct IS NULL THEN "2. unified delivered null"
     WHEN legacy_delivered_ct IS NULL THEN "3. legacy delivered null"
     WHEN unified_delivered_ct > legacy_delivered_ct THEN "4. more unified"
     WHEN unified_delivered_ct < legacy_delivered_ct THEN "5. more legacy"
     ELSE "6.other" END AS match_tpye,
COUNT(*) AS total_visit_ct,
FROM `etsy-data-warehouse-dev.nlao.ud_um_ld_event_ct`
GROUP BY ALL
ORDER BY 1,2,3
;


-- validation 4: unified impression & legacy seen
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ui_li_event_ct` AS (
WITH legacy AS (
SELECT
platform,
module_placement AS _placement_beacon,
visit_id,
SUM(seen) AS seen_ct
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE seen = 1 
AND _date BETWEEN '2025-05-07' AND '2025-05-08'
AND platform IN ('desktop', 'mobile_web')
AND module_placement IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
)
GROUP BY ALL
)
, unified AS (
SELECT
platform,
_placement_beacon,
visit_id,
COUNT(*) AS seen_ct
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'listing_impression'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
GROUP BY ALL
)
SELECT
COALESCE(u._placement_beacon,l._placement_beacon) AS _placement_beacon,
COALESCE(u.platform,l.platform) AS platform,
COALESCE(u.visit_id,l.visit_id) AS visit_id,
u.seen_ct AS unified_seen_ct,
l.seen_ct AS legacy_seen_ct
FROM unified u
FULL OUTER JOIN legacy l
  ON u.visit_id = l.visit_id AND u._placement_beacon = l._placement_beacon AND u.platform = l.platform
)
;

-- comparing unified impression & legacy per visit
SELECT
platform,
_placement_beacon,
CASE WHEN unified_seen_ct = legacy_seen_ct THEN "1. matchup"
     WHEN unified_seen_ct IS NULL THEN "2. unified delivered null"
     WHEN legacy_seen_ct IS NULL THEN "3. legacy delivered null"
     WHEN unified_seen_ct > legacy_seen_ct THEN "4. more unified"
     WHEN unified_seen_ct < legacy_seen_ct THEN "5. more legacy"
     ELSE "6.other" END AS match_tpye,
COUNT(*) AS total_visit_ct,
FROM `etsy-data-warehouse-dev.nlao.ui_li_event_ct`
GROUP BY ALL
ORDER BY 1,2,3
;


-- validation 5: unified click vs legacy click
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.uc_lc_event_ct` AS (
WITH legacy AS (
SELECT
platform,
module_placement AS _placement_beacon,
visit_id,
SUM(clicked) AS clicked_ct
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE clicked = 1 
AND _date BETWEEN '2025-05-07' AND '2025-05-08'
AND platform IN ('desktop', 'mobile_web')
AND module_placement IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
)
GROUP BY ALL
)
, unified AS (
SELECT
platform,
_placement_beacon,
visit_id,
COUNT(*) AS clicked_ct
FROM `etsy-data-warehouse-dev.nlao.recs_visits`
WHERE event_name = 'listing_interaction'
AND platform IN ('desktop', 'mobile_web')
AND _placement_beacon IN (
                                              "home_opfy",
                                              "home_rv",
                                              "home_rf",
                                              "internal_bot",
                                              "external_top",
                                              "pla_top",
                                              "external_bot",
                                              "lp_free_shipping_bundle",
                                              "pla_bot"
) 
GROUP BY ALL
)
SELECT
COALESCE(u._placement_beacon,l._placement_beacon) AS _placement_beacon,
COALESCE(u.platform,l.platform) AS platform,
COALESCE(u.visit_id,l.visit_id) AS visit_id,
u.clicked_ct AS unified_clicked_ct,
l.clicked_ct AS legacy_clicked_ct
FROM unified u
FULL OUTER JOIN legacy l
  ON u.visit_id = l.visit_id AND u._placement_beacon = l._placement_beacon AND u.platform = l.platform
)
;

-- comparing legacy vs unified click data per visit
SELECT
platform,
_placement_beacon,
CASE WHEN unified_clicked_ct = legacy_clicked_ct THEN "1. matchup"
     WHEN unified_clicked_ct IS NULL THEN "2. unified delivered null"
     WHEN legacy_clicked_ct IS NULL THEN "3. legacy delivered null"
     WHEN unified_clicked_ct > legacy_clicked_ct THEN "4. more unified"
     WHEN unified_clicked_ct < legacy_clicked_ct THEN "5. more legacy"
     ELSE "6.other" END AS match_tpye,
COUNT(*) AS total_visit_ct,
FROM `etsy-data-warehouse-dev.nlao.uc_lc_event_ct`
GROUP BY ALL
ORDER BY 1,2,3
;

SELECT

FROM `etsy-data-warehouse-dev.nlao.uc_lc_event_ct`
WHERE  

