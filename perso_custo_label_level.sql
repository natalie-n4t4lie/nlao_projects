CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.knowledge_base.perso_custo_label_level` AS (
-- 1) extract perso/custo keywords from title
-- 2) get number of variation
-- 3) is_personalizable flag
-- 4) if listing has perso/custo keywords AND (number of variation >=1 OR is_personalizable =1)
WITH
pero_custo_listings AS (
 SELECT
   l.listing_id,
   l.shop_id,
   l.title,
   l.variation_count,
   la.is_personalizable
 FROM
   `etsy-data-warehouse-prod.listing_mart.listing_vw` l
 LEFT JOIN
   `etsy-data-warehouse-prod.listing_mart.listing_attributes` la
 ON l.listing_id = la.listing_id
WHERE ((LOWER(title) LIKE '%personali%' AND LOWER(title) NOT LIKE '%personality%')
       OR LOWER(title) LIKE '%monogram%'
       OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
       OR LOWER(title) LIKE '%made to order%' OR LOWER(title) LIKE '%made-to-order%')
       AND (is_personalizable = 1 OR variation_count >= 1)
),
-- extract personalization instruction field from each listings
personalization_instruction as (
SELECT 
  cr.listing_id,
  tr.instructions
FROM `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` tr
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_personalization_current_revisions` cr
ON tr.shop_id = cr.shop_id AND tr.listing_personalization_revision_id = cr.listing_personalization_revision_id
WHERE cr.listing_id in (SELECT LISTING_ID FROM pero_custo_listings)
),
custo_birth_flower as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_birth_flower' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'birth flower|birthflower|birth month flower|birthmonth flower')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower')
),
custo_birth_stone as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_birth_stone' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'birth stone|birthstone|birth month stone|birthmonth stone')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone')
),
custo_zodiac_sign as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_zodiac_sign' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'zodiac|astrology|constellation|star sign')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'zodiac|astrology|constellation|star sign')
      OR (REGEXP_CONTAINS(LOWER(attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(attribute_value), r'taurus'))
),
custo_location as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_location' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'state|province|country|region') 
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'location|state|province|country|region')
      OR (LOWER(title) LIKE "city %" 
          OR LOWER(title) LIKE "city%" 
          OR LOWER(title) LIKE "% city" 
          OR LOWER(title) LIKE "% city %" 
          OR LOWER(title) LIKE "city")
      OR (LOWER(attribute_name) LIKE "city %" 
          OR LOWER(attribute_name) LIKE "city%" 
          OR LOWER(attribute_name) LIKE "% city" 
          OR LOWER(attribute_name) LIKE "% city %" 
          OR LOWER(attribute_name) LIKE "city")
      OR REGEXP_CONTAINS(LOWER(instructions), r'location|province|country|region')
),
custo_initial as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_initial' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'alphabet|letter|initial|character|monogram')
       OR REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character|monogram')
       OR lower(attribute_value) like "% z %"
          OR lower(attribute_value) like "z %"
          OR lower(attribute_value) like "z"
),
custo_moon_phase as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_moon_phase' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'moon phase|moonphase')
       OR REGEXP_CONTAINS(LOWER(attribute_name), r'moon phase|moonphase')
       OR LOWER(attribute_value) LIKE "%waxing gibbous%"
), 
custo_font as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_font' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(attribute_name), r'font|font style|schriftart')
       OR REGEXP_CONTAINS(LOWER(attribute_value), r'arial|times new roman|comic sans')
),
custo_shape as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_shape' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE LOWER(attribute_name) LIKE "%shape%"
),
custo_image_icon as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_image_icon' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE LOWER(attribute_name) LIKE "image" 
       OR LOWER(attribute_name) LIKE "%icon%"
),
custo_pattern as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_pattern' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(attribute_name), r'pattern|fabric|finish')
),
custo_scent as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_scent' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE LOWER(attribute_name) LIKE "%scent%" AND (LOWER(attribute_name) NOT LIKE "%crescent%" AND (LOWER(attribute_name) NOT LIKE "%iridescent%"))
       OR LOWER(attribute_name) LIKE "%flavor%"
       OR (LOWER(title) LIKE '% scent%' AND LOWER(title) NOT LIKE "%unscented%")
),
custo_text as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_text' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE  is_personalizable = 0
       AND (LOWER(attribute_name) LIKE "text" OR LOWER(attribute_name) LIKE "%title%" OR LOWER(attribute_name) LIKE "%wording%")
),
custo_number as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'custo_number' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE (LOWER(attribute_name) LIKE "%number%" OR LOWER(attribute_name) LIKE "%year%" OR LOWER(attribute_name) LIKE "%age%")
      AND LOWER(attribute_name) NOT LIKE "%number of%" AND LOWER(attribute_name) NOT LIKE "%design%"
),
perso_audio as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_audio' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r' audio|soundwave|sound wave')
),
perso_date as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_date' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE is_personalizable = 1
       AND (REGEXP_CONTAINS(LOWER(instructions), r'date|birthday|month|year') OR REGEXP_CONTAINS(LOWER(attribute_name), r'year'))
),
perso_text as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_text' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE is_personalizable = 1
       AND (REGEXP_CONTAINS(LOWER(instructions), r'text|phrase|wording|word|saying|message|letter')
       OR LOWER(attribute_name) LIKE "text" OR LOWER(attribute_name) LIKE "%title%" OR LOWER(attribute_name) LIKE "%wording%")
),
perso_address as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_address' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE is_personalizable = 1
       AND REGEXP_CONTAINS(LOWER(instructions), r'address|latitude')
),
perso_name as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_name' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE (is_personalizable = 1 
        AND (REGEXP_CONTAINS(LOWER(instructions), r'name|character|surname') 
              OR LOWER(instructions) like "%engrav%" 
              OR LOWER(instructions) like "%personali%" 
              OR LOWER(instructions) like "%embroid%" 
              OR LOWER(instructions) like "%custom%" 
              OR LOWER(instructions) like "%monogram%" 
              ))
      OR LOWER(attribute_name) like "%engrav%" 
      OR LOWER(attribute_name) like "%personali%" 
      OR LOWER(attribute_name) like "%embroid%" 
      OR LOWER(attribute_name) like "%custom%" 
      OR LOWER(attribute_name) like "%monogram%" 
),
perso_initial as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_initial' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(instructions), r'initial') 
), 
perso_image as (
SELECT 
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 'perso_image' as perso_custo_label
FROM pero_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE REGEXP_CONTAINS(LOWER(title), r'portrait|logo')
), 
temp_label as (
SELECT current_date() as run_date, * FROM custo_birth_flower	UNION ALL
SELECT current_date() as run_date, * FROM custo_birth_stone	UNION ALL
SELECT current_date() as run_date, * FROM custo_zodiac_sign	UNION ALL
SELECT current_date() as run_date, * FROM custo_location	UNION ALL
SELECT current_date() as run_date, * FROM custo_initial	UNION ALL
SELECT current_date() as run_date, * FROM custo_moon_phase	UNION ALL
SELECT current_date() as run_date, * FROM custo_font	UNION ALL
SELECT current_date() as run_date, * FROM custo_shape	UNION ALL
SELECT current_date() as run_date, * FROM custo_image_icon	UNION ALL
SELECT current_date() as run_date, * FROM custo_pattern	UNION ALL
SELECT current_date() as run_date, * FROM custo_scent	UNION ALL
SELECT current_date() as run_date, * FROM custo_text	UNION ALL
SELECT current_date() as run_date, * FROM custo_number	UNION ALL
SELECT current_date() as run_date, * FROM perso_audio	UNION ALL
SELECT current_date() as run_date, * FROM perso_date	UNION ALL
SELECT current_date() as run_date, * FROM perso_text	UNION ALL
SELECT current_date() as run_date, * FROM perso_address	UNION ALL
SELECT current_date() as run_date, * FROM perso_name	UNION ALL
SELECT current_date() as run_date, * FROM perso_initial	UNION ALL
SELECT current_date() as run_date, * FROM perso_image
)

SELECT * FROM temp_label
UNION ALL 
SELECT 
  current_date() as run_date,
  *,
 'no_label' as perso_custo_label
FROM pero_custo_listings
WHERE listing_id not in (SELECT listing_id FROM temp_label)
)


