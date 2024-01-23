CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.coupon_audience` AS ( 
SELECT 
  sbc.buyer_user_id
  ,case 
      when sbc.coupon_state = 0 then "active"
      when sbc.coupon_state = 1 then "redeemed"
      when sbc.coupon_state = 2 then "cancelled"
      when sbc.coupon_state = 3 then "dismissed"
  else null 
  end as coupon_state
  ,sbc.granted_date
  ,sbc.expire_date
  ,sbc.promotion_id
  ,smp.is_thank_you
  ,CASE WHEN smpo.audience_type = 1 THEN 1 ELSE 0 END AS is_abandon_cart
  ,ats.date AS offer_redemption_date
FROM `etsy-data-warehouse-prod.etsy_shard.shop_buyer_coupon` sbc
JOIN `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion` smp 
  ON sbc.promotion_id = smp.promotion_id
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promoted_offer`   smpo
  ON smp.promotion_id = smpo.promotion_id
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.shop_receipts_promotions` srp
  ON smp.promotion_id = srp.promotion_id
LEFT JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` ats
  ON srp.receipt_id = ats.receipt_id
)
;

-- Visit with TY Coupon available
WITH tyc_visit AS (
SELECT
DISTINCT user_id,
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-dev.nlao.coupon_audience` ca
  ON v.user_id = ca.buyer_user_id
WHERE v._date >= CURRENT_DATE - 31
AND is_thank_you = 1
-- for active offer, find users who visited between offer granted date and offer expiration date
AND ((coupon_state = 'active' AND v._date >= DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND (v._date <=DATE(TIMESTAMP_SECONDS(ca.expire_date)) OR expire_date IS NULL))
-- for redeemed offer, find users who visited between offer granted date and offer redeem date
OR (coupon_state = 'redeemed' AND v._date >= DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND (v._date <=offer_redemption_date))
)
),
aco_visit AS (
SELECT
DISTINCT user_id,
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-dev.nlao.coupon_audience` ca
  ON v.user_id = ca.buyer_user_id
WHERE v._date >= CURRENT_DATE - 31
AND is_abandon_cart = 1
-- for active offer, find users who visited between offer granted date and offer expiration date
AND ((coupon_state = 'active' AND v._date >= DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND (v._date <=DATE(TIMESTAMP_SECONDS(ca.expire_date)) OR expire_date IS NULL))
-- for redeemed offer, find users who visited between offer granted date and offer redeem date
OR (coupon_state = 'redeemed' AND v._date >= DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND (v._date <=offer_redemption_date))
)
)
SELECT
COUNT(DISTINCT user_id) AS signin_visits,
COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM aco_visit) THEN user_id ELSE NULL END) AS aco_visit,
COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM tyc_visit) THEN user_id ELSE NULL END) AS tyc_visit,
FROM `etsy-data-warehouse-prod.weblog.visits` v
WHERE v._date >= CURRENT_DATE - 31
;


-- Visit with Coupon that's granted less than 30 days ago comparing to the time of visit
WITH tyc_visit AS (
SELECT
DISTINCT user_id,
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-dev.nlao.coupon_audience` ca
  ON v.user_id = ca.buyer_user_id
WHERE v._date >= CURRENT_DATE - 31
AND is_thank_you = 1
-- for active offer, find users who visited between offer granted date and offer expiration date
AND ((coupon_state = 'active' 
      AND v._date BETWEEN DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND DATE(TIMESTAMP_SECONDS(ca.granted_date)) + 30 -- visit when the offer granted is less than 30 days old
      AND (v._date <=DATE(TIMESTAMP_SECONDS(ca.expire_date)) OR expire_date IS NULL))-- not expired

-- for redeemed offer, find users who visited between offer granted date and offer redeem date
OR (coupon_state = 'redeemed' 
      AND v._date BETWEEN DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND DATE(TIMESTAMP_SECONDS(ca.granted_date)) + 30 -- visit when the offer granted is less than 30 days old
     AND v._date <= offer_redemption_date)) -- before or on the data of offer redemption
),
aco_visit AS (
SELECT
DISTINCT user_id,
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-dev.nlao.coupon_audience` ca
  ON v.user_id = ca.buyer_user_id
WHERE v._date >= CURRENT_DATE - 31
AND is_abandon_cart = 1
AND ((coupon_state = 'active' 
      AND v._date BETWEEN DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND DATE(TIMESTAMP_SECONDS(ca.granted_date)) + 30 -- visit when the offer granted is less than 30 days old
      AND (v._date <=DATE(TIMESTAMP_SECONDS(ca.expire_date)) OR expire_date IS NULL))-- not expired

-- for redeemed offer, find users who visited between offer granted date and offer redeem date
OR (coupon_state = 'redeemed' 
      AND v._date BETWEEN DATE(TIMESTAMP_SECONDS(ca.granted_date)) AND DATE(TIMESTAMP_SECONDS(ca.granted_date)) + 30 -- visit when the offer granted is less than 30 days old
     AND v._date <= offer_redemption_date)) -- before or on the data of offer redemption
)
SELECT
COUNT(DISTINCT user_id) AS signin_visits,
COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM aco_visit) THEN user_id ELSE NULL END) AS aco_visit,
COUNT(DISTINCT CASE WHEN user_id IN (SELECT user_id FROM tyc_visit) THEN user_id ELSE NULL END) AS tyc_visit,
FROM `etsy-data-warehouse-prod.weblog.visits` v
WHERE v._date >= CURRENT_DATE - 31
;

-- 90-day RPR
create or replace table `etsy-data-warehouse-dev.nlao.purchase` as (
  select distinct
    a.date
    ,a.mapped_user_id
    ,a.listing_id
    ,a.seller_user_id
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_30day_purchase_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.seller_user_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_30day_purhcase_same_shop_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.listing_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_30day_purhcase_same_listing_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id order by unix_date(date(date)) RANGE between 1 following and 60 following) as next_60day_purchase_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.seller_user_id order by unix_date(date(date)) RANGE between 1 following and 60 following) as next_60day_purhcase_same_shop_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.listing_id order by unix_date(date(date)) RANGE between 1 following and 60 following) as next_60day_purhcase_same_listing_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_90day_purchase_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.seller_user_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_90day_purhcase_same_shop_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.listing_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_90day_purhcase_same_listing_count
  FROM `etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
  JOIN `etsy-data-warehouse-prod`.user_mart.user_profile d
    on a.buyer_user_id = d.user_id
  where a.date >= current_date - 365 - 90
      and d.is_seller = 0
  ORDER BY 2,1
); 

select
  count(distinct case when next_30day_purchase_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_30_etsy,
  count(distinct case when next_30day_purhcase_same_shop_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_30_shop,
  count(distinct case when next_30day_purhcase_same_listing_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_30_listing,

    count(distinct case when next_60day_purchase_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_60_etsy,
  count(distinct case when next_60day_purhcase_same_shop_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_60_shop,
  count(distinct case when next_60day_purhcase_same_listing_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_60_listing,

  count(distinct case when next_90day_purchase_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_90_etsy,
  count(distinct case when next_90day_purhcase_same_shop_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_90_shop,
  count(distinct case when next_90day_purhcase_same_listing_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_90_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
;


CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ty_coupon_redeem` AS ( 
SELECT 
  sbc.buyer_user_id
  ,date(timestamp_seconds(sbc.granted_date)) AS granted_date
  ,sbc.promotion_id
  ,srp.receipt_id
  ,ats.date AS offer_redemption_date
FROM `etsy-data-warehouse-prod.etsy_shard.shop_buyer_coupon` sbc
JOIN `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion` smp
  ON sbc.promotion_id = smp.promotion_id
JOIN `etsy-data-warehouse-prod.etsy_shard.shop_receipts_promotions` srp
  ON smp.promotion_id = srp.promotion_id
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` ats
  ON ats.receipt_id = srp.receipt_id
WHERE sbc.coupon_state = 1 -- REDEEMED
  AND is_thank_you = 1 -- TYC
  AND date(timestamp_seconds(sbc.granted_date)) >= CURRENT_DATE - 365 
  AND date(timestamp_seconds(sbc.granted_date)) <= ats.date
)
;-- check for tyc granted in the past year

--TYC
WITH redemption_date_diff AS (
SELECT
  buyer_user_id
  ,promotion_id
  ,receipt_id
  ,MIN(DATE_DIFF(offer_redemption_date,granted_date,day)) AS datediff
FROM `etsy-data-warehouse-dev.nlao.ty_coupon_redeem`
GROUP BY 1,2,3
)
SELECT
avg(datediff) AS avgg,
APPROX_QUANTILES(datediff, 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(datediff, 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(datediff, 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(datediff, 100)[OFFSET(30)] AS percentile_30,
APPROX_QUANTILES(datediff, 100)[OFFSET(40)] AS percentile_40,
APPROX_QUANTILES(datediff, 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(datediff, 100)[OFFSET(60)] AS percentile_60,
APPROX_QUANTILES(datediff, 100)[OFFSET(70)] AS percentile_70,
APPROX_QUANTILES(datediff, 100)[OFFSET(80)] AS percentile_80,
APPROX_QUANTILES(datediff, 100)[OFFSET(90)] AS percentile_90,
APPROX_QUANTILES(datediff, 100)[OFFSET(99)] AS percentile_99
FROM redemption_date_diff
;

WITH redemption_date_diff AS (
SELECT
  buyer_user_id
  ,promotion_id
  ,receipt_id
  ,MIN(DATE_DIFF(offer_redemption_date,granted_date,day)) AS datediff
FROM `etsy-data-warehouse-dev.nlao.ty_coupon_redeem`
GROUP BY 1,2,3
)
SELECT
CASE WHEN datediff <=30 THEN 30
     WHEN datediff <=60 THEN 60
     WHEN datediff <=90 THEN 90
    ELSE 91 END AS datediffs,
COUNT(*) AS redemption_ct
FROM redemption_date_diff
GROUP BY 1
ORDER BY 1 ASC
;

