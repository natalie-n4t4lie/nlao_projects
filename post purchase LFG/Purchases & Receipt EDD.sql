-- Code for [Analytics] Experiment retro - Purchases & Receipt EDD (https://docs.google.com/document/d/1U8aBQq1vTr7SSjP-Ir2YgI6nDHy86K4oiu87r9Mvdwk/edit#)


-- help with order submitted by order status - iOS
with bucketed_users as (
  select
    v.user_id,
    max(case when ab.ab_variant='on' then 1 else 0 end) as is_variant
  from `etsy-data-warehouse-prod.catapult.ab_tests` ab
  JOIN `etsy-data-warehouse-prod.weblog.visits` v
    ON ab.visit_id = v.visit_id
  where ab.ab_test = "mobile_dynamic_config.iphone.BOEPurchaseAndReceiptsStatusRedesign"
  and ab._date between '2023-03-07' and '2023-03-19'
  and v._date between '2023-03-07' and '2023-03-19'
  group by 1
)
select
  c.is_variant,
  a.sit_selected_option,
  CASE WHEN sit_date = order_date THEN "1. on order day"
  WHEN sit_date > order_date and (sit_date < shipped_date or (shipped_date is null and delivered_date is null)) then "2. processing - between order and ship date/ship date empty"
  WHEN sit_date >= shipped_date and (sit_date < delivered_date or (shipped_date is not null and delivered_date is null)) then "3. in transit - between ship date and deliver date/deliver date empty"
  WHEN sit_date >= delivered_date then "4.post shipping - after deliver date"
  END AS order_state,
  CASE WHEN a.shipped_date > sit_date THEN 1 ELSE 0 END AS is_shipped,
  a.is_delivered,
  count(distinct b.buyer_user_id) as n_users,
  count(*) as n_msgs
from `etsy-data-warehouse-prod.analytics.issue_resolution` a
join `etsy-data-warehouse-prod.transaction_mart.all_receipts` b 
  on a.receipt_id=b.receipt_id
join bucketed_users c
  on b.buyer_user_id=c.user_id
where a.sit_date between '2023-03-07' and '2023-03-19' -- there was a detected issue for this user during the experiment
group by 1,2,3,4,5
order by 1,2,3,4,5
;

-- tracking order by order state - iOS
WITH tracking_details AS (
SELECT
DISTINCT 
CASE when beacon.event_source in ('ios')
      and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe ios'
      when beacon.event_source in ('android')
      and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe android'
      else 'undefined' end AS app_platform,
CAST((select value from unnest(beacon.properties.key_value) where key = "receipt_id") AS INT64) AS receipt_id,
date(_partitiontime) as track_date,
visit_id,
beacon.browser_id,
beacon.user_id,
from `etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) >= current_date - 30
AND beacon.event_name = "tracking_history_tapped_from_purchases"
)
SELECT
  app_platform,
  CASE WHEN track_date = order_date THEN "1. on order day"
  WHEN track_date > order_date and (track_date < shipped_date or (shipped_date is null and delivered_date is null)) then "2. processing - between order and ship date/ship date empty"
  WHEN track_date >= shipped_date and (track_date < delivered_date or (shipped_date is not null and delivered_date is null)) then "3. in transit - between ship date and deliver date/deliver date empty"
  WHEN track_date >= delivered_date then "4.post shipping - after deliver date"
  END AS order_state,
  count(visit_id) AS visit_count
FROM tracking_details t
JOIN `etsy-data-warehouse-prod.rollups.receipt_shipping_basics` s
  ON t.receipt_id = s.receipt_id
GROUP BY 1,2
ORDER BY 1,2
;

-- other stats comes from the linked query in https://docs.google.com/spreadsheets/d/19XWmbY6YIY277kcRPZhz9o4VUUhttG5ancjt1nfZARE/edit#gid=0

