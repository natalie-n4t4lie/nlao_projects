-- push enabled user
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.app_open_push_settings as (
  SELECT
    b.beacon.event_source as event_source, 
    b.beacon.user_id as user_id,
    p.value,
    b.beacon.timestamp as event_ts,
    ROW_NUMBER() OVER (PARTITION BY b.beacon.event_source, b.beacon.user_id ORDER BY b.beacon.timestamp DESC) as app_open_rank
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  JOIN UNNEST(b.beacon.properties.key_value) p
    ON p.key IN ("authorized", "notifications_enabled")
  WHERE
  b.beacon.event_name = "notification_settings"
  AND DATE(_PARTITIONTIME) >= current_date - 31
  AND b.beacon.event_source IN ("ios", "android")
  QUALIFY app_open_rank = 1
)
;

-- % of orders that have in_transit status and are eligible for email / push
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND tracking_status = "in_transit"
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;


-- % of orders eligible for push where carrier expected arrival time is tomorrow or EDD is tomorrow
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35 
AND date(timestamp_seconds(expected_delivery_date)) = current_date + 1
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % of orders that have carrier expected delivery date and are eligible for email / push
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35 
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % of orders that have location state = delivery address state are eligible for email / push
-- get push enabled user with an undelivered orders that ship_location_state = delivery_location_state
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35 
AND ship_location_state = delivery_location_state 
AND length(ship_location_state) > 1 
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % of orders that have location state that are eligible for email / push
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35 
AND length(ship_location_state) > 1 
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % orders eligible for push that are international and get out for delivery status
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND origin_country_id != destination_country_id
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % of orders are tracked but not shipped with USPS
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND carrier_name != "USPS"
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- % of orders that have have no tracking information that are eligible for email / push
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
rs.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rs
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON rs.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON rs.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND valid_tracking_added = 0
AND delivered_date IS NULL
AND rs.is_digital = 0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- user has order shipped without tracking and visits purchases page/screen or clicks transactional email/push
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
rs.buyer_user_id AS user_id,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rs
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON rs.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON rs.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND valid_tracking_added = 0
AND delivered_date IS NULL
AND rs.is_digital = 0
GROUP BY 1
)
,visit AS (
  SELECT
    DISTINCT v.user_id,
    start_datetime
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN `etsy-data-warehouse-prod.weblog.events` e
    ON v.visit_id = e.visit_id
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND (event_type IN ("your_purchases","yr_purchases") OR top_channel IN ("email_transactional","push_trans"))
  AND e._date BETWEEN start_date AND end_date
  AND v._date BETWEEN start_date AND end_date
)
,visits_gms as (
  SELECT
    platform, 
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN visit g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.start_datetime
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- users with orders with location info about their order that click transactional emails, click transactional push notifications, or visit the you tab on app or web
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;

WITH target_user AS (
SELECT
s.buyer_user_id AS user_id,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.etsy_shard.shop_shipment` s
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON s.receipt_id = r.receipt_id
WHERE DATE(creation_tsz) >= current_date - 35
AND LENGTH(ship_location_state) > 1
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1
)
,visit AS (
  SELECT
    DISTINCT v.user_id,
    start_datetime
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN `etsy-data-warehouse-prod.weblog.events` e
    ON v.visit_id = e.visit_id
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND (event_type IN ("you_screen","you_tab_viewed") OR top_channel IN ("email_transactional","push_trans"))
  AND e._date BETWEEN start_date AND end_date
  AND v._date BETWEEN start_date AND end_date
)
,visits_gms as (
  SELECT
    platform, 
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN visit g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.start_datetime
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;


-- % of orders with tracking (exclude digital download, order that didn't provide tracking info)
BEGIN
DECLARE start_date DATE default current_date - 5;
DECLARE end_date DATE default current_date - 1;
WITH target_user AS (
SELECT
rs.buyer_user_id AS user_id,
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
MIN(creation_tsz) as earliest_txn,
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` rs
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` r
  ON rs.receipt_id = r.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON rs.buyer_user_id = p.user_id
WHERE DATE(creation_tsz) >= current_date - 35
AND valid_tracking_added = 1
AND delivered_date IS NULL
AND rs.is_digital = 0
GROUP BY 1,2
)
,visits_gms as (
  SELECT
    platform, 
    push_enabled,
    SUM(v.total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits` v 
  JOIN target_user g 
    ON g.user_id = v.user_id
    AND v.start_datetime > g.earliest_txn
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date BETWEEN start_date AND end_date
  GROUP BY 1,2
)
, all_gms as (
  SELECT 
    SUM(total_gms) as total_gms
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE _date BETWEEN start_date and end_date
  AND platform IN ("mobile_web", "desktop", "boe")
)
SELECT
  v.platform,
  push_enabled,
  v.total_gms / a.total_gms
FROM visits_gms v
JOIN all_gms a
  ON 1=1
;

END;

-- push enabled user
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.app_open_push_settings as (
  SELECT
    b.beacon.event_source as event_source, 
    b.beacon.user_id as user_id,
    p.value,
    b.beacon.timestamp as event_ts,
    ROW_NUMBER() OVER (PARTITION BY b.beacon.event_source, b.beacon.user_id ORDER BY b.beacon.timestamp DESC) as app_open_rank
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  JOIN UNNEST(b.beacon.properties.key_value) p
    ON p.key IN ("authorized", "notifications_enabled")
  WHERE
  b.beacon.event_name = "notification_settings"
  AND DATE(_PARTITIONTIME) >= current_date - 31
  AND b.beacon.event_source IN ("ios", "android")
  QUALIFY app_open_rank = 1
)
;

-- total undelivered order
SELECT
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
WHERE s.order_date >= current_date - 35
AND s.delivered_date IS NULL
;

-- % of orders with tracking (exclude digital download, order that didn't provide tracking info)
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND s.valid_tracking_added = 1
GROUP BY 1
;

-- % of orders that have in_transit status and are eligible for email / push
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35
AND s.delivered_date IS NULL
AND ss.tracking_status = "in_transit"
GROUP BY 1
;

-- % of orders eligible for push where carrier expected arrival time is tomorrow or EDD is tomorrow
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND date(timestamp_seconds(expected_delivery_date)) = current_date + 1
GROUP BY 1
;

-- % of orders that have carrier expected delivery date and are eligible for email / push
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND date(timestamp_seconds(expected_delivery_date)) >= current_date AND expected_delivery_date !=0
GROUP BY 1
;

-- % of orders that have location state = delivery address state are eligible for email / push
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND length(ship_location_state) > 1 
AND ship_location_state = delivery_location_state 
GROUP BY 1
;

-- % of orders that have location state that are eligible for email / push
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND length(ship_location_state) > 1 
GROUP BY 1
;


-- % orders eligible for push that are international and get out for delivery status
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND ss.origin_country_id != destination_country_id
GROUP BY 1
;

-- % of orders are tracked but not shipped with USPS
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_shipment` ss
  ON s.receipt_id = ss.receipt_id
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND ss.carrier_name != "USPS"
GROUP BY 1
;

-- % of orders that have have no tracking information that are eligible for email / push
SELECT
CASE WHEN p.value = "true" THEN 1 ELSE 0 END AS push_enabled,
COUNT(DISTINCT s.receipt_id) AS ct
FROM `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
LEFT JOIN `etsy-data-warehouse-dev`.nlao.app_open_push_settings p
  ON s.buyer_user_id = p.user_id
WHERE s.order_date >= current_date - 35 
AND s.delivered_date IS NULL
AND s.valid_tracking_added = 0
GROUP BY 1
;

