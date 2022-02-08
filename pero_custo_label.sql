-- owner: nlao@etsy.com
-- owner_team: hmf-ads-analytics@etsy.com
-- description: perso custo listing label
-- dependency: `etsy-data-warehouse-prod.listing_mart.listing_vw` ,  `etsy-data-warehouse-prod.listing_mart.listing_attributes`, `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes`, `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations`

-- Perso/custo listing definition
-- 1) has at least one variation OR have personalization enabled
-- 2) has perso/custo keywords

-- listing level
CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` AS (

WITH pero_custo_listings AS (
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
WHERE ((LOWER(title) LIKE '%personali%' AND title NOT LIKE '%personality%')
       OR LOWER(title) LIKE '%monogram%'
       OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
       OR LOWER(title) LIKE '%made to order%' OR LOWER(title) LIKE '%made-to-order%')
       AND (is_personalizable = 1 OR variation_count >= 1)
),
-- Joining tables to get 1)variation name 2)variation value 3)personalization instruction
detail_label as (
SELECT
 l.listing_id,
 l.shop_id,
 l.title,
 l.variation_count,
 l.is_personalizable,
 CASE WHEN
      REGEXP_CONTAINS(LOWER(title), r'birth flower|birthflower|birth month flower|birthmonth flower')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower')
     THEN 1
     ELSE 0
   END AS custo_birth_flower,
 CASE WHEN
      REGEXP_CONTAINS(LOWER(title), r'birth stone|birthstone|birth month stone|birthmonth stone')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone')
     THEN 1
     ELSE 0
   END AS custo_birth_stone, 
 CASE WHEN
      REGEXP_CONTAINS(LOWER(title), r'zodiac|astrology|constellation|star sign')
      OR REGEXP_CONTAINS(LOWER(attribute_name), r'zodiac|astrology|constellation|star sign')
      OR (REGEXP_CONTAINS(LOWER(attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(attribute_value), r'taurus'))
     THEN 1
     ELSE 0
   END AS custo_zodiac_sign,
 CASE WHEN
      REGEXP_CONTAINS(LOWER(title), r'state|province|country|region') 
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
     THEN 1
     ELSE 0
   END AS custo_location,
 CASE WHEN
       REGEXP_CONTAINS(LOWER(title), r'alphabet|letter|initial|character')
       OR REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character')
       OR LOWER(title) like "%monogram%"
       OR LOWER(attribute_name) like "%monogram%"
       OR lower(attribute_value) like "% z %"
          OR lower(attribute_value) like "z %"
          OR lower(attribute_value) like "z"
       THEN 1
     ELSE 0
   END AS custo_initial,
 CASE WHEN
       REGEXP_CONTAINS(LOWER(title), r'moon phase|moonphase')
       OR REGEXP_CONTAINS(LOWER(attribute_name), r'moon phase|moonphase')
       OR LOWER(attribute_value) LIKE "%waxing gibbous%"
       THEN 1
     ELSE 0
   END AS custo_moon_phase,
 CASE WHEN
       REGEXP_CONTAINS(LOWER(attribute_name), r'font|font style|schriftart')
       OR REGEXP_CONTAINS(LOWER(attribute_value), r'arial|times new roman|comic sans')
       THEN 1
     ELSE 0
   END AS custo_font,
 CASE WHEN
       LOWER(attribute_name) LIKE "%shape%"
       THEN 1
     ELSE 0
   END AS custo_shape,
  CASE WHEN
       LOWER(attribute_name) LIKE "image" 
       OR LOWER(attribute_name) LIKE "design"
       OR LOWER(attribute_name) LIKE "%icon%"
       THEN 1
     ELSE 0
   END AS custo_image_icon,
  CASE WHEN
       REGEXP_CONTAINS(LOWER(attribute_name), r'pattern|fabric|finish')
       THEN 1
     ELSE 0
   END AS custo_pattern,
   CASE WHEN
       LOWER(attribute_name) LIKE "%scent%" AND (LOWER(attribute_name) NOT LIKE "%crescent%" AND (LOWER(attribute_name) NOT LIKE "%iridescent%"))
       OR LOWER(attribute_name) LIKE "%flavor%"
       OR (LOWER(title) LIKE '% scent%' AND LOWER(title) NOT LIKE "%unscented%")
       THEN 1
     ELSE 0
   END AS custo_scent,
   CASE WHEN
       is_personalizable = 0
       AND (LOWER(attribute_name) LIKE "text" OR LOWER(attribute_name) LIKE "%title%" OR LOWER(attribute_name) LIKE "%wording%")
       THEN 1
     ELSE 0
   END AS custo_text,
   CASE WHEN
       (LOWER(attribute_name) LIKE "%number%" OR LOWER(attribute_name) LIKE "%year%" OR LOWER(attribute_name) LIKE "%age%")
       AND LOWER(attribute_name) NOT LIKE "%number of%" AND LOWER(attribute_name) NOT LIKE "%design%"
       THEN 1
     ELSE 0
   END AS custo_number,
   CASE WHEN
       REGEXP_CONTAINS(LOWER(title), r' audio|soundwave|sound wave')
       THEN 1
     ELSE 0
   END AS perso_audio,
   CASE WHEN
       is_personalizable = 1
       AND (REGEXP_CONTAINS(LOWER(instructions), r'date|birthday|month|year') OR REGEXP_CONTAINS(LOWER(attribute_name), r'year'))
       THEN 1
     ELSE 0
   END AS perso_date,
   CASE WHEN
       is_personalizable = 1
       AND (REGEXP_CONTAINS(LOWER(instructions), r'text|phrase|wording|word|saying|message|letter')
       OR LOWER(attribute_name) LIKE "text" OR LOWER(attribute_name) LIKE "%title%" OR LOWER(attribute_name) LIKE "%wording%")
       THEN 1
     ELSE 0
   END AS perso_text,
   CASE WHEN
       is_personalizable = 1
       AND REGEXP_CONTAINS(LOWER(instructions), r'address|latitude')
       THEN 1
     ELSE 0
   END AS perso_address,
   CASE WHEN
      (is_personalizable = 1 
        AND (REGEXP_CONTAINS(LOWER(instructions), r'name|character') 
            OR LOWER(instructions) LIKE "%personali%"
            OR LOWER(instructions) LIKE "%engrav%"
            OR LOWER(instructions) LIKE "%custom%"
            OR LOWER(instructions) LIKE "%embroid%"
            OR LOWER(instructions) LIKE "%monogram%"))
      OR LOWER(attribute_name) LIKE "%personali%"
            OR LOWER(attribute_name) LIKE "%engrav%"
            OR LOWER(attribute_name) LIKE "%custom%"
            OR LOWER(attribute_name) LIKE "%embroid%"
            OR LOWER(attribute_name) LIKE "%monogram%" 
      THEN 1
     ELSE 0
   END AS perso_name,
   CASE WHEN
      is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(instructions), r'initial')
      THEN 1
     ELSE 0
   END AS perso_initial,
   CASE WHEN
      REGEXP_CONTAINS(LOWER(title), r'portrait|logo')
      THEN 1
     ELSE 0
   END AS perso_image
FROM pero_custo_listings l
LEFT JOIN
   `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN
   `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` lpft USING (listing_id)
)
select 
  current_date() AS run_date,
  listing_id,
  shop_id,
  title,
  variation_count,
  is_personalizable,
  max(custo_birth_flower) as custo_birth_flower,
  max(custo_birth_stone) as custo_birth_stone,
  max(custo_zodiac_sign) as custo_zodiac_sign,
  max(custo_location) as custo_location,
  max(custo_initial) as custo_initial,
  max(custo_moon_phase) as custo_moon_phase,
  max(custo_font) as custo_font,
  max(custo_shape) as custo_shape,
  max(custo_image_icon) as custo_image_icon,
  max(custo_pattern) as custo_pattern,
  max(custo_scent) as custo_scent,
  max(custo_text) as custo_text,
  max(custo_number) as custo_number,
  max(perso_audio) as perso_audio,
  max(perso_date) as perso_date,
  max(perso_text) as perso_text,
  max(perso_address) as perso_address,
  max(perso_name) as perso_name,
  max(perso_initial) as perso_initial,
  max(perso_image) as perso_image
FROM detail_label
GROUP BY 1,2,3,4,5,6
)
;


--Label level

CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label_union` AS (
SELECT current_date() AS run_date,'custo_birth_flower' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_birth_flower = 1
UNION ALL
SELECT current_date() AS run_date,'custo_birth_stone' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_birth_stone = 1
UNION ALL
SELECT current_date() AS run_date,'custo_zodiac_sign' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_zodiac_sign = 1
UNION ALL
SELECT current_date() AS run_date,'custo_location' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_location = 1
UNION ALL
SELECT current_date() AS run_date,'custo_initial' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_initial = 1
UNION ALL
SELECT current_date() AS run_date,'custo_moon_phase' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_moon_phase = 1
UNION ALL
SELECT current_date() AS run_date,'custo_font' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_font = 1
UNION ALL
SELECT current_date() AS run_date,'custo_shape' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_shape = 1
UNION ALL
SELECT current_date() AS run_date,'custo_image_icon' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_image_icon = 1
UNION ALL
SELECT current_date() AS run_date,'custo_pattern' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_pattern = 1
UNION ALL
SELECT current_date() AS run_date,'custo_scent' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_scent = 1
UNION ALL
SELECT current_date() AS run_date,'custo_text' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_text = 1
UNION ALL
SELECT current_date() AS run_date,'custo_number' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_number = 1
UNION ALL
SELECT current_date() AS run_date,'perso_audio' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_audio = 1
UNION ALL
SELECT current_date() AS run_date,'perso_date' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_date = 1
UNION ALL
SELECT current_date() AS run_date,'perso_text' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_text = 1
UNION ALL
SELECT current_date() AS run_date,'perso_address' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_address = 1
UNION ALL
SELECT current_date() AS run_date,'perso_name' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_name = 1
UNION ALL
SELECT current_date() AS run_date,'perso_initial' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_initial = 1
UNION ALL
SELECT current_date() AS run_date,'perso_image' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE perso_image = 1
UNION ALL
SELECT current_date() AS run_date,'no_label' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label` WHERE custo_birth_flower+custo_birth_stone+custo_zodiac_sign+custo_location+custo_initial+custo_moon_phase+custo_font+custo_shape+custo_image_icon+custo_pattern+custo_scent+custo_text+custo_number+perso_audio+perso_date+perso_text+perso_address+perso_name+perso_initial+perso_image = 0
)
;
 
 

