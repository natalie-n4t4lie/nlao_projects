--actions from the purchases page

DECLARE purchases_event_types ARRAY <STRING> DEFAULT [
  "view_receipt",
  "tracking_history_tapped_from_purchases",
  "purchases_screen_search_bar_tapped",
  "help_with_order_button_tapped_from_purchases",
  "shop_header_tapped_purchases",
  "review_item_button_tapped_from_purchases",
  "buy_this_again_button_tapped_purchases",
  "shop_viewed_from_purchase", -- android
  "review_callout_clicked", -- android
  "your_orders_order_tracking", -- android
  "boe_explore_screen_delivered",
  "boe_favorites_backend",
  "boe_homescreen_tab_delivered",
  "you_tab_viewed",
  "cart_view"
  ];

with yr_purchases_browsers as (
  SELECT v.event_source, v.browser_id, v.visit_id, MIN(e.sequence_number) as purchases_page_view
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
    AND v._date = e._date
  WHERE e._date >= current_date - 30
    AND v.platform = "boe"
    AND event_type = "yr_purchases"
  GROUP BY 1, 2, 3
)
, follow_up_events as (
  SELECT
    DISTINCT p.browser_id, e.visit_id, e.event_type
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN yr_purchases_browsers p 
    ON e.visit_id = p.visit_id
    AND e.sequence_number >= p.purchases_page_view
  WHERE e.event_type IN UNNEST(purchases_event_types)
)
SELECT 
  b.event_source,
  (CASE WHEN ev = "view_receipt" THEN "Viewed Receipt"
    WHEN ev = "tracking_history_tapped_from_purchases" or ev = "your_orders_order_tracking" THEN "Viewed Tracking"
    WHEN ev = "help_with_order_button_tapped_from_purchases" THEN "Help With Order"
    WHEN ev IN ("review_item_button_tapped_from_purchases", "review_callout_clicked") THEN "Tapped Item Review CTA"
    WHEN ev IN ("shop_header_tapped_purchases", "shop_viewed_from_purchase") THEN "Viewed Shop from Purchases"
    WHEN ev = "buy_this_again_button_tapped_purchases" THEN "Tapped Buy Again"
    WHEN ev = "purchases_screen_search_bar_tapped" THEN "Tapped Purchase Search Bar"
    WHEN ev IN ("boe_explore_screen_delivered",
  "boe_favorites_backend",
  "boe_homescreen_tab_delivered",
  "you_tab_viewed",
  "cart_view")  THEN "Tapped Lower Other Tabs"
    END) as event,
  AVG(CASE WHEN f.visit_id IS NOT NULL THEN 1 ELSE 0 END) as pct_with_event
FROM yr_purchases_browsers b
JOIN UNNEST(purchases_event_types) ev
  ON 1=1
LEFT JOIN follow_up_events f
  ON ev = f.event_type
  AND f.browser_id = b.browser_id
  AND f.visit_id = b.visit_id
GROUP BY 1, 2
;

-- what % of orders have N items
with receipt_level_count as (
  SELECT t.receipt_id, COUNT(DISTINCT t.listing_id) as n_listings
  FROM `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
  JOIN `etsy-data-warehouse-prod.visit_mart.visits_transactions` tx
    ON tx.transaction_id = t.transaction_id
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` rv
    ON rv.visit_id = tx.visit_id
    AND rv.platform = "boe"
    AND rv._date >= current_date - 30
  WHERE DATE(t.creation_tsz) >= current_date - 30
  GROUP BY 1
)
SELECT
  CASE WHEN n_listings <= 4 THEN n_listings ELSE 5 END as n_listings,
  COUNT(*) as n_orders
FROM receipt_level_count
GROUP BY 1

-- what % of buy again taps actually buy in visit?
with buy_again_taps as (
  SELECT v.visit_id, e.listing_id, MIN(e.sequence_number) as sequence_number
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
    AND v._date = e._date
  WHERE e._date >= current_date - 30
    AND v.platform = "boe"
    AND e.event_type = "buy_this_again_button_tapped_purchases"
    AND REGEXP_CONTAINS(e.listing_id, "[0-9]+$")
  GROUP BY 1, 2
)
, visit_level_purchase_summ as (
  SELECT ba.visit_id, ba.listing_id, MAX(COALESCE(lv.purchased_after_view, 0)) as purchased
  FROM buy_again_taps ba
  LEFT JOIN `etsy-data-warehouse-prod.analytics.listing_views` lv
    ON ba.visit_id = lv.visit_id
    AND lv.listing_id = CAST(ba.listing_id as INT64)
    AND lv.sequence_number >= ba.sequence_number
    AND lv._date >= current_date - 30
  GROUP BY 1, 2
)
SELECT AVG(purchased)
FROM visit_level_purchase_summ

-- bounce rate
with yr_purchases_visits as (
  SELECT DISTINCT v.event_source, v.browser_id, v.visit_id, v.exit_event
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
    AND v._date = e._date
  WHERE e._date >= current_date - 30
    AND v.platform = "boe"
    AND e.event_type = "yr_purchases"
)
SELECT event_source, AVG(CASE WHEN exit_event = "yr_purchases" THEN 1 ELSE 0 END) as exit_rate
FROM yr_purchases_visits
GROUP BY 1

-- how many purchases page visits have an active order
with yr_purchases_visits as (
  SELECT DISTINCT v.event_source, v.user_id, v.visit_id, v.exit_event, TIMESTAMP_MILLIS(e.epoch_ms) as purchases_ts
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
    AND v._date = e._date
  WHERE e._date >= current_date - 30
    AND v.platform = "boe"
    AND e.event_type = "yr_purchases"
)
, visit_active_summary as (
  SELECT v.visit_id, MAX(CASE WHEN r.receipt_id IS NOT NULL THEN 1 ELSE 0 END) as had_active_order
  FROM yr_purchases_visits v
  LEFT JOIN `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` r
    ON r.buyer_user_id = v.user_id
    AND v.purchases_ts BETWEEN r.order_tsz AND timestamp(r.delivered_date + 1)
  GROUP BY 1
)
SELECT AVG(had_active_order)
FROM visit_active_summary

-- what % of purchases page views have no tracking data for the most recent order?
with yr_purchases_visits as (
  SELECT DISTINCT v.event_source, v.user_id, v.visit_id, v.exit_event, TIMESTAMP_MILLIS(e.epoch_ms) as purchases_ts
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
    AND v._date = e._date
  WHERE e._date BETWEEN (current_date - 30) AND (current_date - 14)
    AND v.platform = "boe"
    AND e.event_type = "yr_purchases"
)
, visit_active_summary as (
  SELECT
    v.visit_id,
    user_id IS NOT NULL as has_user_id,
    purchases_ts,
    CASE WHEN r.tracking_added = 1 THEN 0 ELSE 1 END as no_shipping,
    CASE WHEN r.receipt_id IS NOT NULL THEN 1 ELSE 0 END as no_orders,
    ROW_NUMBER() OVER (PARTITION BY v.user_id, v.visit_id, purchases_ts ORDER BY r.order_tsz DESC) as order_rank
  FROM yr_purchases_visits v
  JOIN `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` r
    ON r.buyer_user_id = v.user_id
    AND r.order_tsz < v.purchases_ts
    AND is_digital = 0
  QUALIFY order_rank = 1
)
SELECT
  COUNT(visit_id),
  COUNT(DISTINCT visit_id),
  SUM(no_orders),
  AVG(no_orders),
  SUM(no_shipping),
  AVG(no_shipping)
FROM visit_active_summary

-- you tab actions (some not working for android)
DECLARE lookback INT64 DEFAULT 14;
with you_tab_views as (
  SELECT v.visit_id, v.event_source, MIN(sequence_number) as sequence_number
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
  WHERE
  e._date = current_date - lookback 
  AND v._date = current_date - lookback
  AND v.platform = "boe"
  AND e.event_type IN ("you_tab_viewed", "you_screen")
GROUP BY 1, 2
)
, visit_level_summ as (
SELECT
  y.event_source,
  y.visit_id,
  MAX(CASE WHEN e.event_type = "help_in_app_help_tapped" AND y.event_source = "ios" THEN 1 ELSE 0 END) as help,
  MAX(CASE WHEN e.event_type IN ("user_settings", "update_setting") THEN 1 ELSE 0 END) as settings,
  MAX(CASE WHEN e.event_type IN ("open_create_gift_card", "buy_gift_card_tapped") THEN 1 ELSE 0 END) as gift_card,
  MAX(CASE WHEN e.event_type IN ("yr_purchases", "your_purchases") THEN 1 ELSE 0 END) as purchases,
  MAX(CASE WHEN e.event_type IN ("convo_main", "messages_clicked") THEN 1 ELSE 0 END) as convos,
  MAX(CASE WHEN e.event_type IN ("people_account", "your_account") THEN 1 ELSE 0 END) as profile,
FROM you_tab_views y 
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON y.visit_id = e.visit_id
  AND e.sequence_number > y.sequence_number
  AND e._date >= current_date - lookback
  AND e.event_type IN (
    "help_in_app_help_tapped",  -- help "help_in_app_help_center_tapped",
    "user_settings", -- user settings "update_setting",
    "open_create_gift_card", "buy_gift_card_tapped", -- gift card
    "yr_purchases", "your_purchases", -- purchases
    "convo_main", "messages_clicked", -- messages
    "people_account", "your_account" -- profile
  )
GROUP BY 1, 2
)
SELECT 
event_source,
AVG(help) as help ,
AVG(settings) as settings,
AVG(gift_card) as gift_card,
AVG(purchases) as purchases,
AVG(convos) as convos,
AVG(profile) as profile
FROM visit_level_summ
GROUP BY 1

-- review coverage for purchases page on BOE (58%)

with experiment_counts as (
  SELECT SUM(event_count) as experiment_total
  FROM `etsy-data-warehouse-prod.catapult.browser_event_data_rollup` c
  WHERE
  ab_test = "beat.post_purchase_reengage_v2.experiment"
  AND event_name IN ("submit_review_form_app")
  AND segment = "all"
  AND segmentation = "any"
  AND _date = "2022-12-04"
)
, boundary_total as (
  SELECT SUM(has_review) as review_count_boundary
  FROM `etsy-data-warehouse-prod.rollups.transaction_reviews` 
  WHERE date(review_date) between "2022-11-18" and "2022-12-04"
)
SELECT e.experiment_total / t.review_count_boundary 
FROM boundary_total t
JOIN experiment_counts e ON 1=1 

-- receipt page interaction -- only working for iOS
DECLARE lookback INT64 DEFAULT 14;

with you_tab_views as (
  SELECT v.visit_id, v.event_source, MIN(sequence_number) as sequence_number
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
  WHERE
  e._date = current_date - lookback 
  AND v._date = current_date - lookback
  AND v.platform = "boe"
  AND e.event_type ="view_receipt"
GROUP BY 1, 2
)
, visit_level_summ as (
SELECT
  y.event_source,
  y.visit_id,
  MAX(CASE WHEN event_type = "buy_this_again_button_tapped_receipt" THEN 1 ELSE 0 END) as buy_this_again_button_tapped_receipt,
  MAX(CASE WHEN event_type = "help_with_order_button_new_link_tapped_receipt" THEN 1 ELSE 0 END) as help_with_order_button_new_link_tapped_receipt,
  MAX(CASE
    WHEN event_type = "tracking_history_tapped_from_receipt"
    OR (event_type = "track_package_clicked" AND y.event_source = "android") THEN 1 ELSE 0 END) as tracking_history_tapped_from_receipt,
  MAX(CASE WHEN event_type = "tapped_item_from_receipt" THEN 1 ELSE 0 END) as tapped_item_from_receipt,
  MAX(CASE WHEN event_type = "tapped_shop_name_from_receipt" THEN 1 ELSE 0 END) as tapped_shop_name_from_receipt   
FROM you_tab_views y 
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON y.visit_id = e.visit_id
  AND e.sequence_number > y.sequence_number
  AND e._date >= current_date - lookback
  AND e.event_type IN (
    "buy_this_again_button_tapped_receipt", "buy_this_again_button_tapped_receipts",
    "help_with_order_button_new_link_tapped_receipt",
    "tracking_history_tapped_from_receipt", "track_package_clicked", 
    "tapped_item_from_receipt",
    "tapped_shop_name_from_receipt"
  )
GROUP BY 1, 2
)
SELECT 
event_source,
AVG(buy_this_again_button_tapped_receipt) as buy_this_again_button_tapped_receipt,
AVG(help_with_order_button_new_link_tapped_receipt) as help_with_order_button_new_link_tapped_receipt,
AVG(tracking_history_tapped_from_receipt) as tracking_history_tapped_from_receipt,
AVG(tapped_item_from_receipt) as tapped_item_from_receipt,
AVG(tapped_shop_name_from_receipt) as tapped_shop_name_from_receipt
FROM visit_level_summ
GROUP BY 1

-- percent of you tab views with review/purchases badged
DECLARE lookback INT64 DEFAULT 14;
with you_tab_views as (
  SELECT v.visit_id, v.start_datetime, v.user_id, v.event_source, MIN(sequence_number) as sequence_number
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
  WHERE
  e._date = current_date - lookback 
  AND v._date = current_date - lookback
  AND v.platform = "boe"
  AND e.event_type IN ("you_tab_viewed", "you_screen")
GROUP BY 1, 2, 3, 4
)
, open_orders as (
  SELECT DISTINCT y.visit_id
  FROM you_tab_views y
  JOIN `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
    ON s.buyer_user_id = y.user_id
    AND DATE(y.start_datetime) BETWEEN s.delivered_date AND (s.delivered_date + 100)
  LEFT JOIN `etsy-data-warehouse-prod.rollups.transaction_reviews` r
    ON r.receipt_id = s.receipt_id
    AND r.has_review > 0
    AND r.review_date < y.start_datetime
  WHERE r.receipt_id IS NULL 
)
SELECT AVG(CASE WHEN o.visit_id IS NOT NULL THEN 1 ELSE 0 END) as pct_with_badge
FROM you_tab_views y
LEFT JOIN open_orders o
  ON o.visit_id = y.visit_id

-- gms coverage for transactions after receipt view - really small rn
-- receipt page interaction -- only working for iOS
DECLARE lookback INT64 DEFAULT 14;

with you_tab_views as (
  SELECT v.visit_id, v.event_source, MIN(sequence_number) as sequence_number
  FROM `etsy-data-warehouse-prod.weblog.events` e
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
    ON v.visit_id = e.visit_id
  WHERE
  e._date = current_date - lookback 
  AND v._date = current_date - lookback
  AND v.platform = "boe"
  AND e.event_type ="view_receipt"
  AND event_source = "ios"
GROUP BY 1, 2
)
, visit_level_summ as (
SELECT
  y.event_source,
  y.visit_id,
  event_type,
  MIN(TIMESTAMP_MILLIS(e.epoch_ms)) as event_ts
  -- MAX(CASE WHEN event_type = "buy_this_again_button_tapped_receipt" THEN 1 ELSE 0 END) as buy_this_again_button_tapped_receipt,
  -- MAX(CASE WHEN event_type = "help_with_order_button_new_link_tapped_receipt" THEN 1 ELSE 0 END) as help_with_order_button_new_link_tapped_receipt,
FROM you_tab_views y 
JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON y.visit_id = e.visit_id
  AND e.sequence_number > y.sequence_number
  AND e._date >= current_date - lookback
  AND e.event_type IN (
    "tapped_item_from_receipt",
    "tapped_shop_name_from_receipt"
  )
GROUP BY 1, 2, 3
),
gms_from_path as (
  SELECT
    v.visit_id,
    -- MAX(CASE WHEN v.event_type = "buy_this_again_button_tapped_receipt" THEN 1 ELSE 0 END) as buy_this_again_button_tapped_receipt,
    -- MAX(CASE WHEN v.event_type = "help_with_order_button_new_link_tapped_receipt" THEN 1 ELSE 0 END) as help_with_order_button_new_link_tapped_receipt,
    v.event_type,
    SUM(t.usd_subtotal_price) as gms 
  FROM visit_level_summ v
  JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` tv
    ON tv.visit_id = v.visit_id
  JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` t
    ON t.transaction_id = tv.transaction_id
    AND t.creation_tsz >= v.event_ts
  GROUP BY 1, 2
)
, gms_in_period as (
  SELECT SUM(total_gms) as gms, COUNT(DISTINCT browser_id) as n_browsers
  FROM `etsy-data-warehouse-prod.weblog.recent_visits`
  WHERE platform IN ("mobile_web", "desktop", "boe")
  AND _date >= current_date - lookback
)
SELECT v.event_type, SUM(v.gms), SUM(v.gms) / MAX(p.gms) 
FROM gms_from_path v
JOIN gms_in_period p ON 1=1
GROUP BY 1

-- purchase page reviews impact
-- (91510804 * 1.2 * 0.58 * 0.01 * 0.00000009 * .3 *.65 / 100) * 14.2 billion
