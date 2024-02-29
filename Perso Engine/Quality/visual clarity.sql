-- This table combines Merch V1_1 scores and the existing clarity issues
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.merch_and_clarity_issues` AS (
  SELECT DISTINCT
    m.listing_id
    , m.image_id
    , COALESCE(gms.past_year_gms, 0) AS past_year_gms
    ,CASE WHEN b.score >= 0.5 THEN 1 ELSE 0 END AS has_border
    ,CASE WHEN c.score >= 0.5 THEN 1 ELSE 0 END AS has_collage
    ,CASE WHEN w.score >= 0.7 THEN 1 ELSE 0 END AS has_watermark
    , b.score AS border
    , c.score AS collage
    , w.score AS watermark
    , cv.gamma AS gamma
    , cv.blurry AS is_blurry
    , cv.low_resolution as is_low_resolution
    , cv.low_contrast AS is_low_contrast
    , CASE WHEN b.score > 0.5 OR c.score > 0.5 or w.score > 0.5 THEN 1 ELSE 0 END AS clarity_issue
    , url
  FROM `etsy-data-warehouse-prod.merch_ai.merch_ai_model_predictions_v1_1` m
  JOIN `etsy-data-warehouse-prod.merch_ai.border_model_predictions_v1_0` b
  ON m.listing_id = b.listing_id AND m.image_id = b.image_id
  JOIN `etsy-data-warehouse-prod.merch_ai.collage_model_predictions_v1_0` c
  ON m.listing_id = c.listing_id AND m.image_id = c.image_id
  JOIN `etsy-data-warehouse-prod.merch_ai.watermark_model_predictions_v1_0` w
  ON m.listing_id = w.listing_id AND m.image_id = w.image_id
  JOIN `etsy-data-warehouse-prod.computer_vision.listing_image_paths` lip
  ON m.listing_id = lip.listing_id AND m.image_id = lip.image_id
  JOIN `etsy-data-warehouse-prod.merch_ai.cv_image_features_v1_0` cv
  ON m.listing_id = cv.listing_id AND m.image_id = cv.image_id
  LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_gms` gms
  ON m.listing_id = gms.listing_id
  QUALIFY ROW_NUMBER() OVER(PARTITION BY m.listing_id, m.image_id ORDER BY m.img_update_date DESC) = 1
)
;

-- edge case where one listing has two image
with cte as(
select
listing_id,
count(distinct image_id) as image_ct,
sum(past_year_gms) as gms
from `etsy-data-warehouse-dev.nlao.merch_and_clarity_issues`
group by 1
)
select
image_ct,
sum(gms) as gms,
count(listing_id),
from cte
group by 1
;

--Model score coverage in Search
CREATE OR REPLACE TABLE `etsy-cv-ml-dev.merch_ai.search_impressions` AS (
  WITH purchase_data AS (
    SELECT 
    *
    , CASE WHEN 
        purchased_in_visit_first_click+purchased_in_visit_last_click+purchased_same_day_first_click+purchased_same_day_last_click>=1 
        THEN 1 ELSE 0 END AS purchases
    FROM `etsy-data-warehouse-prod.rollups.organic_impressions`
    WHERE _date > DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
    AND placement = "search"
    --AND page_number = 1
  )
  , search_ctr AS (
    SELECT
      listing_id
      , SUM(n_seen) AS impressions
      , SUM(n_clicked) AS clicks
      , SUM(purchases) AS purchases
      , SUM(purchased_same_day_last_click) / SUM(n_seen) AS conversion_rate
      , SAFE_DIVIDE(SUM(purchased_same_day_last_click), SUM(n_clicked)) AS post_click_conversion_rate
      , SUM(n_clicked) / SUM(n_seen) AS ctr
    FROM purchase_data
    GROUP BY listing_id
  )
SELECT
  s.*
  , m.image_id
  , m.merch
  , m.border
  , m.collage
  , m.watermark
  , m.has_border
  , m.has_collage
  , m.has_watermark
  , m.gamma
  , m.is_blurry
  , m.is_low_resolution
  , m.is_low_contrast
  , m.url
  , m.past_year_gms
  , a.price_usd / 100 AS price_usd
  , a.is_digital
FROM `etsy-data-warehouse-prod.listing_mart.active_listing_vw` a
LEFT JOIN  `etsy-data-warehouse-dev.nlao.merch_and_clarity_issues` m
  ON a.listing_id = m.listing_id
LEFT JOIN search_ctr s
    ON a.listing_id  = s.listing_id
)
;

select
image_id,
listing_id,
from `etsy-cv-ml-dev.merch_ai.search_impressions`
where has_watermark = 1 and is_digital = 1
limit 10
;

SELECT
has_border,
has_collage,
has_watermark,
CASE WHEN is_low_resolution IS TRUE OR is_blurry IS TRUE THEN TRUE ELSE FALSE END AS is_blurry,
is_low_contrast,
is_digital,
SUM(impressions) AS impression,
count(distinct listing_id) as listing_count,
SUM(past_year_gms) AS gms
FROM `etsy-cv-ml-dev.merch_ai.search_impressions`
GROUP BY 1,2,3,4,5,6
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_view_visits` AS (
SELECT DISTINCT 
v._date,
v.user_id,
lv.listing_id,
border,
collage,
watermark,
CASE WHEN (si.is_low_resolution IS TRUE OR si.is_blurry IS TRUE) THEN 1 ELSE 0 END AS is_blurry,
CASE WHEN si.is_low_contrast IS TRUE THEN 1 ELSE 0 END AS is_low_contrast,
si.is_digital,
FROM `etsy-data-warehouse-prod.weblog.visits` v
LEFT JOIN `etsy-data-warehouse-prod.analytics.listing_views` lv
  ON lv.visit_id = v.visit_id
LEFT JOIN `etsy-cv-ml-dev.merch_ai.search_impressions` si
  ON lv.listing_id = si.listing_id
WHERE v._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND lv._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND user_id IS NOT NULL
ORDER BY 2,1
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` AS (
WITH UserVisits AS (
  SELECT
    user_id,
    _date,
    1 AS visit_day,
    COUNT(*) AS visit_count
  FROM `etsy-data-warehouse-prod.weblog.visits` v
  WHERE _date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY) AND CURRENT_DATE()
  AND user_id IS NOT NULL
  GROUP BY 1,2
)
SELECT
  uv.user_id,
  uv._date,
  uv.visit_count,
  SUM(visit_day) OVER (PARTITION BY uv.user_id ORDER BY unix_date(date(uv._date))ASC RANGE BETWEEN 1 FOLLOWING AND 7 FOLLOWING) AS visit_days_in_next_7_days,
  SUM(visit_count) OVER (PARTITION BY uv.user_id ORDER BY unix_date(date(uv._date))ASC RANGE BETWEEN 1 FOLLOWING AND 7 FOLLOWING) AS visits_in_next_7_days
FROM
  UserVisits uv
ORDER BY
  1,2
)
;

SELECT
is_blurry,
CASE WHEN visit_days_in_next_7_days IS NULL THEN 0 ELSE 1 END AS repeat_visit_in_7_days,
count(distinct rv.user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.listing_view_visits` lv
JOIN `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` rv
ON lv.user_id = rv.user_id AND lv._date = rv._date
WHERE lv._date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1,2
;

SELECT
is_low_contrast,
CASE WHEN visit_days_in_next_7_days IS NULL THEN 0 ELSE 1 END AS repeat_visit_in_7_days,
count(distinct rv.user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.listing_view_visits` lv
JOIN `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` rv
ON lv.user_id = rv.user_id AND lv._date = rv._date
WHERE lv._date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1,2
;

SELECT
CAST(CEIL(border * 10) AS INT) AS border_score,
CASE WHEN visit_days_in_next_7_days IS NULL THEN 0 ELSE 1 END AS repeat_visit_in_7_days,
count(distinct rv.user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.listing_view_visits` lv
JOIN `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` rv
ON lv.user_id = rv.user_id AND lv._date = rv._date
WHERE lv._date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1,2
;

SELECT
CAST(CEIL(watermark * 10) AS INT) AS watermark_score,
CASE WHEN visit_days_in_next_7_days IS NULL THEN 0 ELSE 1 END AS repeat_visit_in_7_days,
count(distinct rv.user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.listing_view_visits` lv
JOIN `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` rv
ON lv.user_id = rv.user_id AND lv._date = rv._date
WHERE lv._date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1,2
;

SELECT
CAST(CEIL(collage * 10) AS INT) AS collage_score,
CASE WHEN visit_days_in_next_7_days IS NULL THEN 0 ELSE 1 END AS repeat_visit_in_7_days,
count(distinct rv.user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.listing_view_visits` lv
JOIN `etsy-data-warehouse-dev.nlao.repeat_visit_in7days` rv
ON lv.user_id = rv.user_id AND lv._date = rv._date
WHERE lv._date <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1,2
;



-- repeat purchase rate in 30 days for different labels
create or replace table `etsy-data-warehouse-dev.nlao.purchase` as (
  select distinct
    a.date,
    a.mapped_user_id,
    a.listing_id,
    is_blurry,
    is_low_contrast,
    has_border,
    has_collage,
    has_watermark,
    count(a.mapped_user_id) over (partition by a.mapped_user_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_purchase_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.seller_user_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_purhcase_same_shop_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.listing_id order by unix_date(date(date)) RANGE between 1 following and 30 following) as next_purhcase_same_listing_count
  FROM `etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
  JOIN `etsy-data-warehouse-prod`.user_mart.user_profile d
    on a.buyer_user_id = d.user_id
  LEFT JOIN `etsy-data-warehouse-dev.nlao.merch_and_clarity_issues` q
    on a.listing_id = q.listing_id
  where a.date >= current_date - 60
      and d.is_seller = 0
  ORDER BY 2,1
); 

select
  is_blurry,
  count(distinct mapped_user_id) as denominator,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 30
GROUP BY 1
ORDER BY 1
;

select
  has_watermark,
  count(distinct mapped_user_id) as denominator,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
GROUP BY 1
ORDER BY 1
;

select
  has_border,
  count(distinct mapped_user_id) as denominator,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
GROUP BY 1
;

select
  has_collage,
  count(distinct mapped_user_id) as denominator,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
GROUP BY 1
;

select
  is_low_contrast,
  count(distinct mapped_user_id) as denominator,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
GROUP BY 1
;

-- Search Click Through Rate by buyer segment
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.search_impressions` AS (
  WITH purchase_data AS (
    SELECT 
    s.*
    , v.user_id
    , u.buyer_segment
    ,CASE WHEN v.event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when v.event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when v.event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when v.event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when v.event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform
    , CASE WHEN 
        purchased_in_visit_first_click+purchased_in_visit_last_click+purchased_same_day_first_click+purchased_same_day_last_click>=1 
        THEN 1 ELSE 0 END AS purchases
    FROM `etsy-data-warehouse-prod.rollups.organic_impressions` s
    JOIN `etsy-data-warehouse-prod.weblog.visits` v
      USING (visit_id)
    LEFT JOIN `etsy-data-warehouse-prod.rollups.buyer_basics` u
      ON u.mapped_user_id = v.user_id
    WHERE s._date > DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
    AND v._date > DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
    AND placement = "search"
    --AND page_number = 1
  )
  , search_ctr AS (
    SELECT
      app_platform
      ,listing_id
      , SUM(n_seen) AS impressions
      , SUM(n_clicked) AS clicks
      , SUM(purchases) AS purchases
      , SUM(purchased_same_day_last_click) / SUM(n_seen) AS conversion_rate
      , SAFE_DIVIDE(SUM(purchased_same_day_last_click), SUM(n_clicked)) AS post_click_conversion_rate
      , SUM(n_clicked) / SUM(n_seen) AS ctr
    FROM purchase_data
    GROUP BY 1,2
  )
SELECT
  s.*
  , m.image_id
  , m.merch
  , m.has_border
  , m.has_collage
  , m.has_watermark
  , m.border
  , m.collage
  , m.watermark
  , m.gamma
  , m.is_blurry
  , m.is_low_resolution
  , m.is_low_contrast
  , m.url
  , m.past_year_gms
  , a.price_usd / 100 AS price_usd
  , a.is_digital
FROM search_ctr s
JOIN `etsy-data-warehouse-dev.nlao.merch_and_clarity_issues` m
ON m.listing_id  = s.listing_id
JOIN `etsy-data-warehouse-prod.listing_mart.active_listing_vw` a
ON a.listing_id = m.listing_id
)
;

SELECT
  app_platform
  ,CASE WHEN is_low_resolution IS TRUE OR is_blurry IS TRUE THEN TRUE ELSE FALSE END AS is_blurry
  , AVG(ctr) AS avg_ctr
  , SUM(impressions) AS impressions
  , AVG(past_year_gms) AS avg_past_year_gms
  , SUM(purchases) AS purchases
  , AVG(post_click_conversion_rate) AS avg_post_click_conversion_rate
  , AVG(conversion_rate) AS conversion_rate
  , SUM(price_usd * purchases) / SUM(purchases) AS avg_purchase_price
  , AVG(price_usd) AS avg_price_usd
FROM `etsy-data-warehouse-dev.nlao.search_impressions`
GROUP BY 1, 2
ORDER BY 2, 1;


SELECT
  app_platform
  ,CAST(CEIL(collage * 10) AS INT) AS collage_score
  , AVG(ctr) AS avg_ctr
  , SUM(impressions) AS impressions
  , AVG(past_year_gms) AS avg_past_year_gms
  , SUM(purchases) AS purchases
  , AVG(post_click_conversion_rate) AS avg_post_click_conversion_rate
  , AVG(conversion_rate) AS conversion_rate
  , SUM(price_usd * purchases) / SUM(purchases) AS avg_purchase_price
  , AVG(price_usd) AS avg_price_usd
FROM `etsy-data-warehouse-dev.nlao.search_impressions`
GROUP BY 1, 2
ORDER BY 2, 1;


SELECT
  app_platform
  ,CAST(CEIL(watermark * 10) AS INT) AS watermark_score
  , AVG(ctr) AS avg_ctr
  , SUM(impressions) AS impressions
  , AVG(past_year_gms) AS avg_past_year_gms
  , SUM(purchases) AS purchases
  , AVG(post_click_conversion_rate) AS avg_post_click_conversion_rate
  , AVG(conversion_rate) AS conversion_rate
  , SUM(price_usd * purchases) / SUM(purchases) AS avg_purchase_price
  , AVG(price_usd) AS avg_price_usd
FROM `etsy-data-warehouse-dev.nlao.search_impressions`
GROUP BY 1, 2
ORDER BY 2, 1;
