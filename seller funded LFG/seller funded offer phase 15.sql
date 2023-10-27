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
      `etsy-data-warehouse-prod.listing_mart.listings_active`             la
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
shop_id,
COALESCE(ssd.total_day_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_day_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_day_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.listing_mart.listings_active` l
LEFT JOIN shop_sale_days ssd ON l.shop_id = ssd.shop_id
LEFT JOIN shop_sale_days_shop_wide ssds ON l.shop_id = ssds.shop_id
LEFT JOIN shop_sale_days_selected_listing ssdl ON l.shop_id = ssdl.shop_id
)
;
END
;

------------------------------------------------------------------------------------------------------------------------------------------------

-- listing sale day in a year
-- create a table that calculate days on sale in three scenarios:
-- 1. sale days for shopwide sale ONLY
-- 2. sale days for selected listing ONLY
-- 3. sale days for both shopwide sale and selected listing
BEGIN

CREATE TEMP TABLE shop_sale_days AS (
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
CREATE TEMP TABLE shop_sale_days_shop_wide AS (
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
CREATE TEMP TABLE shop_sale_days_selected_listing AS (
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
listing,
COALESCE(ssd.total_day_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_day_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_day_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.listing_mart.listings_active` l
LEFT JOIN shop_sale_days ssd ON l.listing_id = ssd.listing_id
LEFT JOIN shop_sale_days_shop_wide ssds ON l.listing_id = ssds.listing_id
LEFT JOIN shop_sale_days_selected_listing ssdl ON l.listing_id = ssdl.listing_id
)
;
END
;

------------------------------------------------------------------------------------------------------------------------------------------------

-- How often are shops setting up sales? For a given shop, how many days in a year was this shop on sale?
SELECT
CASE WHEN total_days_on_sale_all = 0 THEN '0'
      WHEN total_days_on_sale_all BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale_all BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_all BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_all BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_all BETWEEN 181 AND 270 THEN '181-270'
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
COUNT(shop_id) AS shop_ct
FROM `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop`
GROUP BY 1
;

-- How often are listings on sale? For a given listing, how many days in a year was this listing on sale?
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
      `etsy-data-warehouse-prod.listing_mart.listings_active`             la
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
shop_id,
COALESCE(ssd.total_day_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_day_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_day_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.listing_mart.listings_active` l
LEFT JOIN shop_sale_days ssd ON l.shop_id = ssd.shop_id
LEFT JOIN shop_sale_days_shop_wide ssds ON l.shop_id = ssds.shop_id
LEFT JOIN shop_sale_days_selected_listing ssdl ON l.shop_id = ssdl.shop_id
)
;
END
;

------------------------------------------------------------------------------------------------------------------------------------------------

-- listing sale day in a year
-- create a table that calculate days on sale in three scenarios:
-- 1. sale days for shopwide sale ONLY
-- 2. sale days for selected listing ONLY
-- 3. sale days for both shopwide sale and selected listing
BEGIN

CREATE TEMP TABLE shop_sale_days AS (
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
CREATE TEMP TABLE shop_sale_days_shop_wide AS (
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
CREATE TEMP TABLE shop_sale_days_selected_listing AS (
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
listing,
COALESCE(ssd.total_day_on_sale,0) AS total_days_on_sale_all,
COALESCE(ssds.total_day_on_sale,0) AS total_days_on_sale_storewide,
COALESCE(ssdl.total_day_on_sale,0) AS total_days_on_sale_selectlisting,
FROM `etsy-data-warehouse-prod.listing_mart.listings_active` l
LEFT JOIN shop_sale_days ssd ON l.listing_id = ssd.listing_id
LEFT JOIN shop_sale_days_shop_wide ssds ON l.listing_id = ssds.listing_id
LEFT JOIN shop_sale_days_selected_listing ssdl ON l.listing_id = ssdl.listing_id
)
;
END
;

------------------------------------------------------------------------------------------------------------------------------------------------

-- How often are shops setting up sales? For a given shop, how many days in a year was this shop on sale?
SELECT
CASE WHEN total_days_on_sale_all = 0 THEN '0'
      WHEN total_days_on_sale_all BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale_all BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_all BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_all BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_all BETWEEN 181 AND 270 THEN '181-270'
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
COUNT(shop_id) AS shop_ct
FROM `etsy-data-warehouse-dev.nlao.total_days_on_sale_shop`
GROUP BY 1,2,3
;

SELECT
CASE WHEN total_days_on_sale_all = 0 THEN '0'
      WHEN total_days_on_sale_all BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale_all BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_all BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_all BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_all BETWEEN 181 AND 270 THEN '181-270'
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
GROUP BY 1,2,3
;

-- For shops that are on sale more than 50% (180 days +) of the year, what is the average duration of these sales?
WITH cte AS (
SELECT distinct 
promotion_id,
date_diff(end_date,start_date,day) AS duration
FROM `etsy-data-warehouse-dev.nlao.shop_sale_shop_and_listing` sl
JOIN `etsy-data-warehouse-dev.nlao.shop_sale_days` sd
      ON sl.shop_id = sd.shop_id
WHERE total_days_on_sale_all >=180
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
WHERE total_days_on_sale_all >=180
)
SELECT
duration,
count(promotion_id)
FROM cte
GROUP BY 1
;

-- listing view representation
SELECT
CASE WHEN total_days_on_sale_all = 0 THEN '0'
      WHEN total_days_on_sale_all BETWEEN 1 AND 30 THEN '1-30'
      WHEN total_days_on_sale_all BETWEEN 31 AND 60 THEN '31-60'
      WHEN total_days_on_sale_all BETWEEN 61 AND 90 THEN '61-90'
      WHEN total_days_on_sale_all BETWEEN 91 AND 180 THEN '91-180'
      WHEN total_days_on_sale_all BETWEEN 181 AND 270 THEN '181-270'
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
COUNT(*) AS listing_ct
FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
LEFT JOIN `etsy-data-warehouse-dev.nlao.total_days_on_sale_listing` USING (listing_id)
WHERE lv._date BETWEEN '2022-08-01' AND '2023-08-01'
GROUP BY 1,2,3
;

-- repeatly on sale listing
-- find listings views that fulfills the following criteria:
      -- viewers are sign-in
      -- viewer viewed a listing at least twice and viewer view the same listing again after 48 hour and within 7 days
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.repeat_lising_views AS (
with listing_view AS (
SELECT
i.listing_id,
i.epoch_ms,
v.user_id
FROM `etsy-data-warehouse-prod`.analytics.listing_views i
JOIN `etsy-data-warehouse-prod`.weblog.visits v USING (visit_id)
WHERE v._date BETWEEN '2023-08-01' AND '2023-08-31'
      AND i._date BETWEEN '2023-08-01' AND '2023-08-31'
)
SELECT 
  i.user_id,
  i.listing_id,
  timestamp_millis(i.epoch_ms) AS first_viewed_at,
  timestamp_millis(i2.epoch_ms) AS second_viewed_at
FROM listing_view i
JOIN listing_view i2 ON i.user_id = i2.user_id
                  AND i.listing_id = i2.listing_id
                  AND timestamp_millis(i2.epoch_ms) > TIMESTAMP_ADD(timestamp_millis(i.epoch_ms), INTERVAL 48 HOUR)
                  AND timestamp_millis(i2.epoch_ms) <= TIMESTAMP_ADD(timestamp_millis(i.epoch_ms), INTERVAL 7 DAY)
)
;

-- filter for users who viewed the listing that has a sale ending in 48 hours
WITH first_with_48hr_sale AS (
SELECT
rlv.*
FROM `etsy-data-warehouse-dev`.nlao.repeat_lising_views rlv
JOIN `etsy-data-warehouse-prod`.rollups.listings_on_sale_by_day ls using (listing_id)
WHERE ls.end_date <= TIMESTAMP_ADD(rlv.first_viewed_at, INTERVAL 48 HOUR) AND ls.promotion_type IN (2,4)
GROUP BY rlv.listing_id, user_id, first_viewed_at, second_viewed_at
)
-- filter from previous table for users who viewed the listing again and has a sale that started within 7 days
,second_with_another_sale AS (
SELECT
rlv.*,
FROM first_with_48hr_sale rlv
JOIN `etsy-data-warehouse-prod`.rollups.listings_on_sale_by_day ls using (listing_id)
WHERE ls.start_date >= TIMESTAMP_SUB(rlv.second_viewed_at, INTERVAL 7 DAY) AND ls.promotion_type IN (2,4)
)
-- check the count of users that fits both criteria
select
count(distinct user_id) as user_ct
from second_with_another_sale
;--2720523

-- # of views who view the listing with a sale ending 
SELECT
count(distinct user_id) as user_view_listing
FROM `etsy-data-warehouse-prod`.analytics.listing_views i
JOIN `etsy-data-warehouse-prod`.weblog.visits v USING (visit_id)
WHERE v._date BETWEEN '2023-08-01' AND '2023-08-31'
      AND i._date BETWEEN '2023-08-01' AND '2023-08-31'
;--user_view_listing
-- 40647995

select 2720523/40647995; -- 6.69%


-- method 2
with listing_view AS (
SELECT
i.listing_id,
i.epoch_ms,
v.user_id
FROM `etsy-data-warehouse-prod`.analytics.listing_views i
JOIN `etsy-data-warehouse-prod`.weblog.visits v USING (visit_id)
WHERE v._date BETWEEN '2023-08-01' AND '2023-08-31'
      AND i._date BETWEEN '2023-08-01' AND '2023-08-31'
)
,listing_has_sale_end_in_48hr_start_in_7days as (
SELECT
i.listing_id,
i.epoch_ms,
i.user_id,
MAX(CASE WHEN promotion_type IN (2,4) 
             AND end_date <= TIMESTAMP_ADD(timestamp_millis(i.epoch_ms), INTERVAL 48 HOUR) -- has a sale ending in 48 hours
    THEN 1 ELSE 0 END) AS has_sale_ending_in_48hr,
MAX(CASE WHEN promotion_type IN (2,4) -- sale promotion type
            AND start_date <= TIMESTAMP_ADD(timestamp_millis(i.epoch_ms), INTERVAL 7 DAY) -- has a sale starts within 7 days
            AND start_date >= TIMESTAMP_ADD(timestamp_millis(i.epoch_ms), INTERVAL 48 HOUR) -- has a sale starts after 48 hours
    THEN 1 ELSE 0 END) AS has_sale_start_in_7days
FROM listing_view i
LEFT JOIN `etsy-data-warehouse-prod`.rollups.listings_on_sale_by_day s USING (listing_id)
GROUP BY 1,2,3
)
,final as (
SELECT 
a.listing_id,
a.user_id,
a.epoch_ms as first_view_at,
b.epoch_ms as second_view_at,
has_sale_ending_in_48hr,
has_sale_start_in_7days
FROM listing_has_sale_end_in_48hr_start_in_7days a
LEFT JOIN listing_view b
  ON a.user_id = b.user_id
  AND timestamp_millis(b.epoch_ms) > TIMESTAMP_ADD(timestamp_millis(a.epoch_ms), INTERVAL 48 HOUR)
  AND timestamp_millis(b.epoch_ms) <= TIMESTAMP_ADD(timestamp_millis(a.epoch_ms), INTERVAL 7 DAY)
where has_sale_ending_in_48hr = 1 AND has_sale_start_in_7days = 1
)

SELECT
count(l.user_id) as view_listing_user_ct,
count(f.has_sale_ending_in_48hr) as view_repeatly_onsale_listing_user_ct
FROM listing_view l
LEFT JOIN final f USING (user_id)
;


select
case when discount_amount is null then 0 else 1 end as on_sale,
case when referring_page_event is null then "landing" 
when referring_page_event in ("search","async_listings_search") then "search"
else referring_page_event end as referring_page,
case when 
referring_page_event is null then "landing"
when r.module_placement is not null then "recommendations"
when lv.ref_tag like "listing_page_ad_row%" then "listing page ad"
when lv.ref_tag like "sc_gallery%" then "paid search"
when lv.ref_tag like "sr_gallery%" then "organic search"
when lv.ref_tag is not null then "other ref_tag" end as ref_tag,
count(*) as percent_of_listing_views
from `etsy-data-warehouse-prod.rollups.listing_view_with_promotion_flag` l
JOIN `etsy-data-warehouse-prod.analytics.listing_views` lv on l.epoch_ms = lv.epoch_ms AND l.listing_id = lv.listing_id AND l.visit_id = lv.visit_id
left join `etsy-data-warehouse-prod.static.recsys_module_mapping` r on lv.ref_tag like r.ref_tag
where lv._date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
group by 1,2,3
;

