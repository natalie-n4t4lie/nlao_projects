WITH cte AS (
SELECT
a.listing_id,
SUM(CASE WHEN li.multiple_variations >= 0.8 THEN 1 ELSE 0 END) AS multi_variant_ct,
SUM(CASE WHEN li.color_chart >= 0.6 THEN 1 ELSE 0 END) AS color_chart_ct,
SUM(CASE WHEN li.infographic >= 0.6 THEN 1 ELSE 0 END) AS infographic_ct,
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id)
GROUP BY 1
)
SELECT
multi_variant_ct,
color_chart_ct,
infographic_ct,
COUNT(listing_id) AS listing_ct
FROM cte
GROUP BY ALL
;

SELECT
COUNT(DISTINCT a.listing_id) AS total_listing,
COUNT(DISTINCT CASE WHEN li.infographic >= 0.6 OR li.color_chart >= 0.6 OR li.multiple_variations >= 0.8 THEN listing_id ELSE NULL END) AS pallete_listing_count
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id)
;

-- multi_variant
WITH multi_variant AS (
SELECT
listing_id,
image_count,
cast(img_rank as int64) as multi_variant_rank
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.listing_mart.listing_images_active` a USING (listing_id)
WHERE li.multiple_variations >= 0.8 AND is_active = 1
)
SELECT
image_count,
multi_variant_rank,
COUNT(listing_id) AS listing_ct
FROM multi_variant
WHERE multi_variant_rank <=image_count
GROUP BY ALL
;

-- color_chart 
WITH color_chart AS (
SELECT
listing_id,
image_count,
cast(img_rank as int64) as color_cart_rank
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.listing_mart.listing_images_active` a USING (listing_id)
WHERE li.color_chart >= 0.6 AND is_active = 1
)
SELECT
image_count,
color_cart_rank,
COUNT(listing_id) AS listing_ct
FROM color_chart
WHERE color_cart_rank <=image_count
GROUP BY ALL
;

-- infographic
WITH infographic AS (
SELECT
listing_id,
image_count,
cast(img_rank as int64) as infographic_rank
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.listing_mart.listing_images_active` a USING (listing_id)
WHERE li.infographic >= 0.6
)
SELECT
image_count,
infographic_rank,
COUNT(listing_id) AS listing_ct
FROM infographic
WHERE infographic_rank <=image_count
GROUP BY ALL
;

-- any palette image
WITH palette AS (
SELECT DISTINCT
listing_id,
image_count,
cast(img_rank as int64) as palette_rank
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.listing_mart.listing_images_active` a USING (listing_id)
WHERE li.infographic >= 0.6 OR li.color_chart >= 0.6 OR li.multiple_variations >= 0.8 
)
SELECT
image_count,
palette_rank,
COUNT(listing_id) AS listing_ct
FROM palette
WHERE palette_rank <=image_count
GROUP BY ALL
;
