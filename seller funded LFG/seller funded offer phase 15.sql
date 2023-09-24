-- create table with sale information
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` AS (
WITH shop_wide AS (
--listings on shopwide sale
SELECT
      smp.shop_id,
      smp.promotion_id,
      la.listing_id,
      timestamp_seconds(smp.start_date) as start_date,
      timestamp_seconds(smp.end_date) as end_date,
      1 as is_sitewide,
      smp.promotion_type,
      case 
        when smp.promotion_type = 1 then "x off entire order"
        when smp.promotion_type = 2 then "% off entire order"
        when smp.promotion_type = 3 then "% off on shipping, entire order"
        when smp.promotion_type = 6 then "% off on domestic shipping, entire order"
        when smp.promotion_type = 4 then "% off select items"
        when smp.promotion_type = 7 then "% off on shipping, select items"
        when smp.promotion_type = 8 then "% off on domestic shipping, select items"
        else null 
     end as promotion_type_name,
     smp.reward_percent_discount_on_order,
     smp.reward_percent_discount_on_items_in_set,
     smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set as discount_amount,
     case when  smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set >= 20 then 1 else 0 end as is_20_or_more,
     case when  smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set >= 25 then 1 else 0 end as is_25_or_more
FROM
      `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion`    smp
INNER JOIN
      `etsy-data-warehouse-prod.rollups.active_listing_basics`             la
  ON
      smp.shop_id = la.shop_id
WHERE
      smp.discoverability_type = 2 -- sale
      and smp.promotion_type in (2,4)
  -- shopwide
  AND NOT EXISTS (
    SELECT  1
    FROM    `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion_listing`    smpl
    WHERE   smp.shop_id       = smpl.shop_id 
      AND   smp.promotion_id  = smpl.promotion_id
  )
  -- sale happens BETWEEN '2022-08-01' AND '2023-08-01' 
  AND timestamp_seconds(smp.start_date) BETWEEN '2022-08-01' AND '2023-08-01'
  AND timestamp_seconds(smp.end_date) <= '2023-08-01'
  AND timestamp_seconds(smp.start_date) <= timestamp_seconds(smp.end_date)
),

listing_specific as (
SELECT
      smp.shop_id,
      smp.promotion_id,
      smpl.listing_id,
      timestamp_seconds(smp.start_date) as start_date,
      timestamp_seconds(smp.end_date) as end_date,
      0 as is_sitewide,
            smp.promotion_type,
      case 
        when smp.promotion_type = 1 then "x off entire order"
        when smp.promotion_type = 2 then "% off entire order"
        when smp.promotion_type = 3 then "% off on shipping, entire order"
        when smp.promotion_type = 6 then "% off on domestic shipping, entire order"
        when smp.promotion_type = 4 then "% off select items"
        when smp.promotion_type = 7 then "% off on shipping, select items"
        when smp.promotion_type = 8 then "% off on domestic shipping, select items"
        else null 
     end as promotion_type_name,
     smp.reward_percent_discount_on_order,
     smp.reward_percent_discount_on_items_in_set,
     smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set as discount_amount,
     case when  smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set >= 20 then 1 else 0 end as is_20_or_more,
     case when  smp.reward_percent_discount_on_order + smp.reward_percent_discount_on_items_in_set >= 25 then 1 else 0 end as is_25_or_more
FROM
      `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion`    smp
INNER JOIN
      `etsy-data-warehouse-prod.etsy_shard.seller_marketing_promotion_listing`             smpl
  ON
        smp.shop_id       = smpl.shop_id
    AND smp.promotion_id  = smpl.promotion_id
WHERE
      smp.discoverability_type = 2 -- sale
      and smp.promotion_type in (2,4)
  -- sale happens BETWEEN '2022-08-01' AND '2023-08-01' 
  AND timestamp_seconds(smp.start_date) BETWEEN '2022-08-01' AND '2023-08-01'
  AND timestamp_seconds(smp.end_date) <= '2023-08-01'
  AND timestamp_seconds(smp.start_date) <= timestamp_seconds(smp.end_date)
)

(select * from shop_wide)
union all
(select * from listing_specific)
)
;



------------------------------------------------------------------------------------------------------------------------------------------------
-- create a table that calculate days on sale in three scenarios:
-- 1. sale days for shopwide sale ONLY
-- 2. sale days for selected listing ONLY
-- 3. sale days for both shopwide sale and selected listing
BEGIN

CREATE TEMP TABLE shop_sale_days AS (
WITH intervals AS (
  SELECT
    shop_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  GROUP BY
    shop_id
),

sorted_intervals AS (
  SELECT
    shop_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY shop_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    shop_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    shop_id, start_time
)

SELECT
  shop_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  shop_id
ORDER BY
  shop_id
)
;

-- store-wide sale only
CREATE TEMP TABLE shop_sale_days_shop_wide AS (
WITH intervals AS (
  SELECT
    shop_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  WHERE is_sitewide = 1
  GROUP BY
    shop_id
),

sorted_intervals AS (
  SELECT
    shop_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY shop_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    shop_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    shop_id, start_time
)

SELECT
  shop_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  shop_id
ORDER BY
  shop_id
)
;

-- selected listing sale only
CREATE TEMP TABLE shop_sale_days_selected_listing AS (
WITH intervals AS (
  SELECT
    shop_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  WHERE is_sitewide = 0
  GROUP BY
    shop_id
),

sorted_intervals AS (
  SELECT
    shop_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY shop_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    shop_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    shop_id, start_time
)

SELECT
  shop_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  shop_id
ORDER BY
  shop_id
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop` AS (
SELECT 
DISTINCT 
l.shop_id,
COALESCE(ssd.total_days_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_days_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_days_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN shop_sale_days ssd ON l.shop_id = ssd.shop_id
LEFT JOIN shop_sale_days_shop_wide ssds ON l.shop_id = ssds.shop_id
LEFT JOIN shop_sale_days_selected_listing ssdl ON l.shop_id = ssdl.shop_id
)
;

BEGIN
------------------------------------------------------------------------------------------------------------------------------------------------
-- listing sale day in a year
-- create a table that calculate days on sale in three scenarios:
-- 1. sale days for shopwide sale ONLY
-- 2. sale days for selected listing ONLY
-- 3. sale days for both shopwide sale and selected listing

CREATE TEMP TABLE listing_sale_days AS (
WITH intervals AS (
  SELECT
    listing_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  GROUP BY
    listing_id
),

sorted_intervals AS (
  SELECT
    listing_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY listing_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    listing_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    listing_id, start_time
)

SELECT
  listing_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  listing_id
ORDER BY
  listing_id
)
;

-- store-wide sale only
CREATE TEMP TABLE listing_sale_days_shop_wide AS (
WITH intervals AS (
  SELECT
    listing_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  WHERE is_sitewide = 1
  GROUP BY
    listing_id
),

sorted_intervals AS (
  SELECT
    listing_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY listing_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    listing_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    listing_id, start_time
)

SELECT
  listing_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  listing_id
ORDER BY
  listing_id
)
;

-- selected listing sale only
CREATE TEMP TABLE listing_sale_days_selected_listing AS (
WITH intervals AS (
  SELECT
    listing_id,
    MIN(start_date) AS start_time,
    MAX(end_date) AS end_time
  from  `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing`
  WHERE is_sitewide = 0
  GROUP BY
    listing_id
),

sorted_intervals AS (
  SELECT
    listing_id,
    start_time,
    end_time,
    LAG(end_time) OVER (PARTITION BY listing_id ORDER BY start_time) AS prev_end_time
  FROM
    intervals
),

merged_intervals AS (
  SELECT
    listing_id,
    start_time,
    MAX(end_time) AS end_time
  FROM
    sorted_intervals
  WHERE
    start_time > COALESCE(prev_end_time, '1900-01-01')
  GROUP BY
    listing_id, start_time
)

SELECT
  listing_id,
  SUM(DATE_DIFF(end_time, start_time, DAY)) + 1 AS total_days_on_sale
FROM
  merged_intervals
GROUP BY
  listing_id
ORDER BY
  listing_id
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.total_days_on_sale_listing` AS (
SELECT DISTINCT
l.listing_id,
COALESCE(ssd.total_days_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_days_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_days_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
LEFT JOIN listing_sale_days ssd ON l.listing_id = ssd.listing_id
LEFT JOIN listing_sale_days_shop_wide ssds ON l.listing_id = ssds.listing_id
LEFT JOIN listing_sale_days_selected_listing ssdl ON l.listing_id = ssdl.listing_id
)
;
END
;


------------------------------------------------------------------------------------------------------------------------------------------------

-- Graph 1.1
SELECT
CASE WHEN total_days_on_sale_all = 0 THEN '0'
      WHEN total_days_on_sale_all BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale_all BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_all BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_all BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_all BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale_all,
CASE WHEN total_days_on_sale_storewide = 0 THEN '0'
      WHEN total_days_on_sale_storewide BETWEEN 0 AND 30 THEN '1-30'
      WHEN total_days_on_sale_storewide BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_storewide BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_storewide BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_storewide BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale_storewide,
CASE WHEN total_days_on_sale_selectlisting = 0 THEN '0'
      WHEN total_days_on_sale_selectlisting BETWEEN 0 AND 30 THEN '1-30'
      WHEN total_days_on_sale_selectlisting BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_selectlisting BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_selectlisting BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_selectlisting BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale_selectlisting,
COUNT(shop_id) AS shop_ct
FROM `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop`
GROUP BY 1,2,3
;

SELECT
CASE WHEN total_days_on_sale_storewide = 0 THEN '0'
      ELSE '1' END AS total_days_on_sale_storewide,
CASE WHEN total_days_on_sale_selectlisting = 0 THEN '0'
      ELSE '1' END AS total_days_on_sale_selectlisting,
COUNT(shop_id) AS shop_ct
FROM `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop`
GROUP BY 1,2
;

SELECT
CASE WHEN total_days_on_sale = 0 THEN '0'
      WHEN total_days_on_sale BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale,
CASE WHEN total_days_on_sale_storewide = 0 THEN '0'
      WHEN total_days_on_sale_storewide BETWEEN 0 AND 30 THEN '1-30'
      WHEN total_days_on_sale_storewide BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_storewide BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_storewide BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_storewide BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale_storewide,
CASE WHEN total_days_on_sale_selectlisting = 0 THEN '0'
      WHEN total_days_on_sale_selectlisting BETWEEN 0 AND 30 THEN '1-30'
      WHEN total_days_on_sale_selectlisting BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_selectlisting BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_selectlisting BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_selectlisting BETWEEN 181 AND 270 THEN '181-270'
      ELSE '271+' END AS total_days_on_sale_selectlisting,
COUNT(listing_id) AS listing_ct
FROM `etsy-data-warehouse-dev.nlao.total_days_on_sale_listing`
GROUP BY 1
;




-- For shops that are on sale more than 50% (180 days +) of the year, what is the average duration of these sales?
WITH cte AS (
SELECT distinct 
promotion_id,
date_diff(end_date,start_date,day) AS duration
FROM `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` sl
JOIN `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop` sd
      ON sl.shop_id = sd.shop_id
WHERE total_days_on_sale >=180
)
SELECT
avg(duration)
FROM cte
;

WITH cte AS (
SELECT distinct 
promotion_id,
date_diff(end_date,start_date,day) AS duration
FROM `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` sl
JOIN `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop` sd
      ON sl.shop_id = sd.shop_id
WHERE total_days_on_sale_storewide >=180
)
SELECT
avg(duration)
FROM cte
;

WITH cte AS (
SELECT distinct 
promotion_id,
date_diff(end_date,start_date,day) AS duration
FROM `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` sl
JOIN `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop` sd
      ON sl.shop_id = sd.shop_id
WHERE total_days_on_sale_selectlisting >=180
)
SELECT
avg(duration)
FROM cte
;

-- sale duration distribution for always-on-sale shop
WITH cte AS (
SELECT distinct 
promotion_id,
date_diff(end_date,start_date,day) AS duration
FROM `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` sl
JOIN `etsy-data-warehouse-dev.nlao.shop_sale_days` sd
      ON sl.shop_id = sd.shop_id
WHERE total_days_on_sale >=180
)
SELECT
duration,
count(promotion_id)
FROM cte
GROUP BY 1
;



