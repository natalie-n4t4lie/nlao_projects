-- OVERALL COVERAGE
SELECT COUNT(DISTINCT listing_id) as active_listing_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;--106022904

-- WITHOUT CONFIDENCE THRESHOLD
SELECT
COUNT(DISTINCT listing_id) AS cvimage_listing_count,
COUNT(DISTINCT listing_id)/106022904 AS cvimage_coverage
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
;-- 96500195 | 0.91018252999370775

-- WITH CONFIDENCE THRESHOLD
SELECT
COUNT(DISTINCT listing_id) AS cvimage_listing_count,
COUNT(DISTINCT listing_id)/106022904 AS cvimage_coverage
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE li.in_isolation >= 0.6 
OR li.multiple_variations >= 0.8
OR li.size_and_scale >= 0.6
OR li.styled_lifestyle_or_in_context >= 0.6
OR li.zoom >= 0.8
OR li.size_chart >= 0.6
OR li.color_chart >= 0.6
OR li.infographic >= 0.6
OR li.white_background >= 0.6
OR li.in_packaging >= 0.8
OR li.has_humans >= 0.6
;-- 94666846| 0.894


-- OVERALL HERO IMAGE COVERAGE
SELECT COUNT(DISTINCT listing_id) as active_listing_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;--106022904

-- WITHOUT CONFIDENCE THREHOLD
SELECT
COUNT(DISTINCT listing_id) AS cvimage_listing_count,
COUNT(DISTINCT listing_id)/106022904 AS cvimage_coverage
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
WHERE li.img_rank = '1'
;-- 95023514 | 0.89625458665044677

-- WITH CONFIDENCE THRESHOLD
SELECT
COUNT(DISTINCT listing_id) AS cvimage_listing_count,
COUNT(DISTINCT listing_id)/106022904 AS cvimage_coverage
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE li.img_rank='1'
AND (li.in_isolation >= 0.6 
OR li.multiple_variations >= 0.8
OR li.size_and_scale >= 0.6
OR li.styled_lifestyle_or_in_context >= 0.6
OR li.zoom >= 0.8
OR li.size_chart >= 0.6
OR li.color_chart >= 0.6
OR li.infographic >= 0.6
OR li.white_background >= 0.6
OR li.in_packaging >= 0.8
OR li.has_humans >= 0.6)
;-- 90503074 | 0.853

#COVERAGE OF EACH IMAGE CLASS BY HERO IMAGE
SELECT
COUNT(DISTINCT CASE WHEN li.in_isolation >= 0.6 THEN listing_id ELSE NULL END) AS in_isolation_listing,
COUNT(DISTINCT CASE WHEN li.multiple_variations >= 0.8 THEN listing_id ELSE NULL END) AS multiple_variations_listing,
COUNT(DISTINCT CASE WHEN li.size_and_scale >= 0.6 THEN listing_id ELSE NULL END) AS size_and_scale_listing,
COUNT(DISTINCT CASE WHEN li.styled_lifestyle_or_in_context >= 0.6 THEN listing_id ELSE NULL END) AS styled_lifestyle_or_in_context_listing,
COUNT(DISTINCT CASE WHEN li.zoom >= 0.8 THEN listing_id ELSE NULL END) AS zoom_listing,
COUNT(DISTINCT CASE WHEN li.size_chart >= 0.6 THEN listing_id ELSE NULL END) AS size_chart_listing,
COUNT(DISTINCT CASE WHEN li.color_chart >= 0.6 THEN listing_id ELSE NULL END) AS color_chart_listing,
COUNT(DISTINCT CASE WHEN li.infographic >= 0.6 THEN listing_id ELSE NULL END) AS infographic_listing,
COUNT(DISTINCT CASE WHEN li.white_background >= 0.6 THEN listing_id ELSE NULL END) AS white_background_listing,
COUNT(DISTINCT CASE WHEN li.in_packaging >= 0.8 THEN listing_id ELSE NULL END) AS in_packaging_listing,
COUNT(DISTINCT CASE WHEN li.has_humans >= 0.6 THEN listing_id ELSE NULL END) AS has_humans_listing,
COUNT(DISTINCT listing_id) AS active_listing_count
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE li.img_rank = '1'
;


#COVERAGE OF EACH IMAGE CLASS
SELECT
COUNT(DISTINCT CASE WHEN li.in_isolation >= 0.6 THEN listing_id ELSE NULL END) AS in_isolation_listing,
COUNT(DISTINCT CASE WHEN li.multiple_variations >= 0.8 THEN listing_id ELSE NULL END) AS multiple_variations_listing,
COUNT(DISTINCT CASE WHEN li.size_and_scale >= 0.6 THEN listing_id ELSE NULL END) AS size_and_scale_listing,
COUNT(DISTINCT CASE WHEN li.styled_lifestyle_or_in_context >= 0.6 THEN listing_id ELSE NULL END) AS styled_lifestyle_or_in_context_listing,
COUNT(DISTINCT CASE WHEN li.zoom >= 0.8 THEN listing_id ELSE NULL END) AS zoom_listing,
COUNT(DISTINCT CASE WHEN li.size_chart >= 0.6 THEN listing_id ELSE NULL END) AS size_chart_listing,
COUNT(DISTINCT CASE WHEN li.color_chart >= 0.6 THEN listing_id ELSE NULL END) AS color_chart_listing,
COUNT(DISTINCT CASE WHEN li.infographic >= 0.6 THEN listing_id ELSE NULL END) AS infographic_listing,
COUNT(DISTINCT CASE WHEN li.white_background >= 0.6 THEN listing_id ELSE NULL END) AS white_background_listing,
COUNT(DISTINCT CASE WHEN li.in_packaging >= 0.8 THEN listing_id ELSE NULL END) AS in_packaging_listing,
COUNT(DISTINCT CASE WHEN li.has_humans >= 0.6 THEN listing_id ELSE NULL END) AS has_humans_listing,
COUNT(DISTINCT listing_id) AS active_listing_count
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
;

#COVERAGE OF EACH IMAGE CLASS BY CATEGORY
SELECT
top_category,
COUNT(DISTINCT CASE WHEN li.in_isolation >= 0.6 THEN listing_id ELSE NULL END) AS in_isolation_listing,
COUNT(DISTINCT CASE WHEN li.multiple_variations >= 0.8 THEN listing_id ELSE NULL END) AS multiple_variations_listing,
COUNT(DISTINCT CASE WHEN li.size_and_scale >= 0.6 THEN listing_id ELSE NULL END) AS size_and_scale_listing,
COUNT(DISTINCT CASE WHEN li.styled_lifestyle_or_in_context >= 0.6 THEN listing_id ELSE NULL END) AS styled_lifestyle_or_in_context_listing,
COUNT(DISTINCT CASE WHEN li.zoom >= 0.8 THEN listing_id ELSE NULL END) AS zoom_listing,
COUNT(DISTINCT CASE WHEN li.size_chart >= 0.6 THEN listing_id ELSE NULL END) AS size_chart_listing,
COUNT(DISTINCT CASE WHEN li.color_chart >= 0.6 THEN listing_id ELSE NULL END) AS color_chart_listing,
COUNT(DISTINCT CASE WHEN li.infographic >= 0.6 THEN listing_id ELSE NULL END) AS infographic_listing,
COUNT(DISTINCT CASE WHEN li.white_background >= 0.6 THEN listing_id ELSE NULL END) AS white_background_listing,
COUNT(DISTINCT CASE WHEN li.in_packaging >= 0.8 THEN listing_id ELSE NULL END) AS in_packaging_listing,
COUNT(DISTINCT CASE WHEN li.has_humans >= 0.6 THEN listing_id ELSE NULL END) AS has_humans_listing,
COUNT(DISTINCT listing_id) AS active_listing_count
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
GROUP BY 1
;

-- CONVERSION/ENGAGEMENT
WITH 
listing_conversion_temp as (
SELECT 
  l.run_date,
  l.visit_id,
  TIMESTAMP_SECONDS(l.run_date) AS date,
  l.detected_region,
  l.listing_id,
  l.detected_region AS buyer_region,
  MAX(l.purchased_in_visit) AS purchased_in_visit,
  COUNT(*) AS views
FROM `etsy-data-warehouse-prod.analytics.listing_views` l 
WHERE l._date >= date_sub(current_date(), INTERVAL 1 YEAR) 
  AND listing_id in (select listing_id FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` WHERE gms_percentile <= 10)
GROUP BY 1,2,3,4,5,6
),
gms_views AS (
    SELECT
        a.run_date,
        a.visit_id,
        a.date,
        a.listing_id,
        a.purchased_in_visit,
        a.views,
        COUNT(transaction_id) as transactions,
        SUM(b.gms_net) AS gms_net
    FROM listing_conversion_temp a
    LEFT JOIN `etsy-data-warehouse-prod.visit_mart.visits_transactions` b
        ON a.visit_id = b.visit_id AND a.listing_id = b.listing_id AND a.purchased_in_visit = 1
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
  display_name,
  SUM(purchased_in_visit) AS purchased_in_visit,
  SUM(views) AS total_views,
  COUNT(*) AS distinct_views,
  SUM(transactions) as transactions,
  SUM(gms_net) AS gms
FROM gms_views 
JOIN `etsy-data-warehouse-prod.knowledge_base.listing_interests` USING (listing_id)
WHERE _date<='2021-12-16' AND attribute_type='Animal'
GROUP BY 1
;


-- Variation Usage: size chart, multiple variations, color chart
SELECT
COUNT(DISTINCT CASE WHEN variation_count >= 1 OR is_personalizable = 1 THEN listing_id ELSE NULL END) AS listing_w_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
WHERE listing_id in (SELECT
listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
WHERE size_chart >= 0.6 OR multiple_variations >= 0.8 OR color_chart >= 0.6)
;

SELECT
COUNT(DISTINCT CASE WHEN variation_count >= 1 OR is_personalizable = 1 THEN listing_id ELSE NULL END) AS listing_w_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
;

# GRANULAR VIEW
-- SIZE CHART: VARIATION UTILIZATION RATE
SELECT
COUNT(DISTINCT CASE WHEN 
  REGEXP_CONTAINS(lower(instructions),r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement')
  OR REGEXP_CONTAINS(lower(attribute_name),r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement') 
    THEN listing_id 
    ELSE NULL END) AS listing_w_size_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` USING (listing_id)
LEFT JOIN `etsy-data-warehouse-dev.nlao.personalization_instruction` USING (listing_id)
WHERE listing_id in (SELECT listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
WHERE size_chart >= 0.6)
;
-- COLOR CHART: VARIATION UTILIZATION RATE
SELECT
COUNT(DISTINCT CASE WHEN 
  REGEXP_CONTAINS(lower(instructions),r'color|colour|couleur|größe|farbe')
  OR REGEXP_CONTAINS(lower(attribute_name),r'color|colour|couleur|größe|farbe') 
    THEN listing_id ELSE NULL END) AS listing_w_color_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` USING (listing_id)
LEFT JOIN `etsy-data-warehouse-dev.nlao.personalization_instruction` USING (listing_id)
WHERE listing_id in (SELECT
listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
WHERE color_chart >= 0.6)
;
-- MULTIPLE VARIATION: VARIATION UTILIZATION RATE
SELECT
COUNT(DISTINCT CASE WHEN variation_count >= 1 OR is_personalizable = 1 THEN listing_id ELSE NULL END) AS listing_w_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` USING (listing_id)
WHERE listing_id in (SELECT
listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
WHERE multiple_variations >= 0.8)
;

-- MULTIPLE VARAITION DETECTED BUT NO VARIATIONS LISTINGS BY CATEGORY
SELECT
alb.top_category, 
count(listing_id) as listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
WHERE listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` WHERE multiple_variations >= 0.8)
    AND variation_count < 1 
    AND is_personalizable = 0
GROUP BY 1
;
-- MULTIPLE VARAITION DETECTED BUT NO VARIATIONS LISTINGS BY CATEGORY
SELECT
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
WHERE listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` WHERE multiple_variations >= 0.8)
    AND variation_count < 1 
    AND is_personalizable = 0
    AND alb.top_category like 'craft%'
limit 50
;

-- HOME AND LIVING EXAMPLE OF MULTIPLE VARIATIONS LISTINGS
SELECT
listing_id
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
WHERE listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` WHERE multiple_variations >= 0.8)
    AND variation_count < 1 
    AND is_personalizable = 0
    AND alb.top_category='home_and_living'
limit 50
;
