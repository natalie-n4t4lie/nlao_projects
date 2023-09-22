-- repeatly on sale listing
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

WITH CTE AS (
SELECT
rlv.*,
MAX(CASE WHEN ls.end_date <= TIMESTAMP_ADD(rlv.first_viewed_at, INTERVAL 48 HOUR) AND ls.promotion_type IN (2,4) THEN 1 ELSE 0 END) AS first_sale_ends_in_48hr
FROM `etsy-data-warehouse-dev`.nlao.repeat_lising_views rlv
LEFT JOIN `etsy-data-warehouse-prod`.rollups.listings_on_sale_by_day ls using (listing_id)
GROUP BY rlv.listing_id, user_id, first_viewed_at, second_viewed_at
)
SELECT COUNT(user_id) AS views_ct,
COUNT(case when first_sale_ends_in_48hr = 1 then user_id else null end) AS user_see_sale_end_in_48hr,
FROM cte
;-- # of buyers who view the listing with a sale ending in 48 hours


SELECT
count(distinct user_id) as user_view_listing
FROM `etsy-data-warehouse-prod`.analytics.listing_views i
JOIN `etsy-data-warehouse-prod`.weblog.visits v USING (visit_id)
WHERE v._date BETWEEN '2023-08-01' AND '2023-08-31'
      AND i._date BETWEEN '2023-08-01' AND '2023-08-31'
;





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





