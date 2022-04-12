
################## OVERALL COVERAGE #####################
-- ACTIVE LISTING WITHOUT CONFIDENCE THRESHOLD
SELECT 
COUNT(listing_id) AS active_listing_ct,
COUNT(CASE WHEN listing_id IN (
    SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
    ) 
    THEN listing_id 
    ELSE NULL 
    END) AS active_listing_w_imag_class_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;--104867230, 94816384

SELECT 94816384/104867230;--90.4%

-- ACTIVE LISTING WITH CONFIDENCE THRESHOLD
SELECT 
COUNT(listing_id) AS active_listing_ct,
COUNT(CASE WHEN listing_id IN (
    SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li 
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
    ) 
    THEN listing_id 
    ELSE NULL END) AS active_listing_w_imag_class_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;-- 104867230, 93797480

SELECT 93797480 / 104867230;--89.4%

-- LISTING WITHOUT IMAGE CLASS
-- BY IMAGE COUNT
SELECT
image_count,
COUNT(listing_id) as listing_ct,
COUNT(CASE WHEN listing_id NOT IN (
    SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
    ) 
    THEN listing_id 
    ELSE NULL 
    END) AS no_class_listing_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
GROUP BY 1
ORDER BY 1 ASC
;

-- BY CATEGORY
SELECT
top_category,
count(listing_id) as listing_ct,
COUNT(CASE WHEN listing_id NOT IN (
    SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
    ) 
    THEN listing_id 
    ELSE NULL 
    END) AS no_class_listing_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
GROUP BY 1
ORDER BY 1 ASC
;

-- BY ORIGINAL CREATE DATE
SELECT
EXTRACT(YEAR FROM original_create_date),
count(listing_id) as listing_ct,
COUNT(CASE WHEN listing_id NOT IN (
    SELECT listing_id FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
    ) 
    THEN listing_id 
    ELSE NULL 
    END) AS no_class_listing_ct
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
GROUP BY 1
ORDER BY 1 ASC
;

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

################## COVERAGE OF EACH IMAGE CLASS #####################
-- BY THUMBNAIL IMAGE
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
;

-- BY ALL IMAGE
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
WHERE (li.in_isolation >= 0.6 
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
;

-- BY CATEGORY
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
WHERE (li.in_isolation >= 0.6 
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
GROUP BY 1
;

################## VARIATION USAGE #####################
-- SIZE CHART / MULTIPLE VARIATIONS / COLOR CHART
SELECT
COUNT(CASE WHEN 
      variation_count >= 1 
      OR is_personalizable = 1 
    THEN listing_id 
    ELSE NULL 
      END) AS active_listing_w_variation,
COUNT(listing_id) AS active_listing_count,
COUNT(CASE WHEN 
    (variation_count >= 1 OR is_personalizable = 1) 
    AND listing_id in (
      SELECT listing_id 
      FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
      WHERE size_chart >= 0.6 OR multiple_variations >= 0.8 OR color_chart >= 0.6)
      THEN listing_id 
      ELSE NULL 
        END) AS active_image_class_listing_w_variation,
COUNT(CASE WHEN listing_id in (
      SELECT listing_id
      FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
      WHERE size_chart >= 0.6 OR multiple_variations >= 0.8 OR color_chart >= 0.6)
      THEN listing_id 
      ELSE NULL 
        END) AS active_image_class_listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` USING (listing_id)
;-- 42124246, 104867230, 11597311, 16707742

SELECT 42124246/ 104867230; --40.2%
SELECT 11597311/ 16707742; -- 69.4%

-- GRANULAR VIEW, SIZE CHART: VARIATION UTILIZATION RATE
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
;-- 4827372, 6222940

SELECT  4827372 / 6222940; -- 77.6%

-- GRANULAR VIEW, COLOR CHART: VARIATION UTILIZATION RATE
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
;-- 2517145, 3274117

SELECT  2517145 / 3274117; --76.9%

-- GRANULAR VIEW, MULTIPLE VARIATION: VARIATION UTILIZATION RATE
SELECT
COUNT(DISTINCT CASE WHEN variation_count >= 1 OR is_personalizable = 1 THEN listing_id ELSE NULL END) AS listing_w_variation,
COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_attributes`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` USING (listing_id)
WHERE listing_id in (SELECT
listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization`
WHERE multiple_variations >= 0.8)
; --  5487669, 9658927

SELECT  5487669 / 9658927; -- 56.8%

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

-- EXAMPLES FOR MULTIPLE VARAITION DETECTED BUT NO VARIATIONS LISTINGS IN CRAFT SUPPLIES
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

-- EXAMPLES FOR MULTIPLE VARAITION DETECTED BUT NO VARIATIONS LISTINGS IN HOME AND LIVING
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
