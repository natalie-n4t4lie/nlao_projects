CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_level` AS (

-- 1) extract perso/custo keywords from title
-- 2) get number of variation
-- 3) is_personalizable flag
-- 4) if listing has perso/custo keywords AND (number of variation >=1 OR is_personalizable =1)
WITH
personalization_instruction as (
SELECT 
  cr.listing_id,
  tr.instructions
FROM `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` tr
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_personalization_current_revisions` cr
ON tr.shop_id = cr.shop_id AND tr.listing_personalization_revision_id = cr.listing_personalization_revision_id
),
pero_custo_label AS (
  SELECT
    l.listing_id,
    l.shop_id,
    l.title,
    l.variation_count,
    CASE WHEN 
        (LOWER(title) LIKE '%personali%' AND title NOT LIKE '%personality%') 
        OR LOWER(title) LIKE '%monogram%' 
        OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%') 
        OR LOWER(title) LIKE '%made to order%' OR LOWER(title) LIKE '%made-to-order%' 
        THEN 1
      ELSE 0
    END AS title_text_custom,
    CASE WHEN ((LOWER(title) LIKE '%personali%' AND title NOT LIKE '%personality%') 
        OR LOWER(title) LIKE '%monogram%' 
        OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%') 
        OR LOWER(title) LIKE '%made to order%' OR LOWER(title) LIKE '%made-to-order%') AND 
        (is_personalizable = 1 OR variation_count >= 1)
      THEN 1 
      ELSE 0
    END AS perso_custo_flag,
    la.is_personalizable AS attribute_personalizable,
    lva.attribute_name,
    lva.attribute_value,
    pi.instructions
  FROM
    `etsy-data-warehouse-prod.listing_mart.listing_vw` l
  LEFT JOIN 
    `etsy-data-warehouse-prod.listing_mart.listing_attributes` la
  ON l.listing_id = la.listing_id
  LEFT JOIN 
    `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva
  ON l.listing_id = lva.listing_id
  LEFT join personalization_instruction pi
  ON l.listing_id = lva.listing_id
),
perso_custo_detail_label as (
SELECT 
  listing_id,
  shop_id,
  title,
  variation_count,
  title_text_custom,
  attribute_personalizable,
  attribute_name,
  attribute_value,
  instructions,
  perso_custo_flag, 
  CASE WHEN 
        (perso_custo_flag = 1)
        AND (REGEXP_CONTAINS(LOWER(title), r'birth flower|birthflower|birth month flower|birthmonth flower')
        OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower'))
      THEN 1
      ELSE 0
    END AS custo_birth_flower,
  CASE WHEN 
        (perso_custo_flag = 1)
        AND (REGEXP_CONTAINS(LOWER(title), r'birth stone|birthstone|birth month stone|birthmonth stone')
        OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone'))
      THEN 1
      ELSE 0
    END AS custo_birth_stone,  
  CASE WHEN 
        (perso_custo_flag = 1) 
        AND (REGEXP_CONTAINS(LOWER(title), r'zodiac|astrology|constellation|star sign') 
        OR REGEXP_CONTAINS(LOWER(attribute_name), r'zodiac|astrology|constellation|star sign')
        OR (REGEXP_CONTAINS(LOWER(attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(attribute_value), r'taurus')))
      THEN 1
      ELSE 0
    END AS custo_zodiac_sign,
  CASE WHEN 
       (perso_custo_flag = 1) 
       AND ((REGEXP_CONTAINS(LOWER(title), r'state|country|region') OR REGEXP_CONTAINS(LOWER(attribute_name), r'state|country|region'))
       OR (LOWER(title) LIKE "city %" OR LOWER(title) LIKE "city%" OR LOWER(title) LIKE "% city" OR LOWER(title) LIKE "% city %" OR LOWER(title) LIKE "city")
       OR (LOWER(attribute_name) LIKE "city %" OR LOWER(attribute_name) LIKE "city%" OR LOWER(attribute_name) LIKE "% city" OR LOWER(attribute_name) LIKE "% city %" OR LOWER(attribute_name) LIKE "city") 
       OR (LOWER(attribute_name) LIKE "state %" OR LOWER(attribute_name) LIKE "state%" OR LOWER(attribute_name) LIKE "% state" OR LOWER(attribute_name) LIKE "% state %" OR LOWER(attribute_name) LIKE "state") 
       OR (LOWER(attribute_name) LIKE "country %" OR LOWER(attribute_name) LIKE "country%" OR LOWER(attribute_name) LIKE "% country" OR LOWER(attribute_name) LIKE "% country %" OR LOWER(attribute_name) LIKE "country")
       OR (LOWER(attribute_name) LIKE "region %" OR LOWER(attribute_name) LIKE "region%" OR LOWER(attribute_name) LIKE "% region" OR LOWER(attribute_name) LIKE "% region %" OR LOWER(attribute_name) LIKE "region"))
      THEN 1
      ELSE 0
    END AS custo_location,
  CASE WHEN
        (perso_custo_flag = 1) 
        AND (REGEXP_CONTAINS(LOWER(title), r'alphabet|letter|initial|character')
        OR REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character')) 
        THEN 1
      ELSE 0
    END AS custo_initial,
  CASE WHEN
        (perso_custo_flag = 1) 
        AND (REGEXP_CONTAINS(LOWER(title), r'moon phase') OR REGEXP_CONTAINS(LOWER(title), r'moonphase')
        OR REGEXP_CONTAINS(LOWER(attribute_name), r'moon phase') OR REGEXP_CONTAINS(LOWER(attribute_name), r'moonphase')
        OR LOWER(attribute_value) LIKE "%waxing gibbous%")
        THEN 1
      ELSE 0
    END AS custo_moon_phase,
  CASE WHEN
        (perso_custo_flag = 1) 
        AND ((REGEXP_CONTAINS(LOWER(attribute_value), r'font|schriftart') AND LOWER(attribute_name) NOT LIKE "%color%" AND LOWER(attribute_name) NOT LIKE "%colour%")  
        OR (REGEXP_CONTAINS(LOWER(attribute_value), r'arial|times new roman|comic sans')))
        THEN 1
      ELSE 0
    END AS custo_font,
  CASE WHEN
        (perso_custo_flag = 1)
        AND (LOWER(attribute_name) LIKE "%shape%")
        THEN 1
      ELSE 0
    END AS custo_shape,
   CASE WHEN
        (perso_custo_flag = 1)
        AND (LOWER(attribute_name) LIKE "image" OR LOWER(attribute_name) LIKE "%icon%")
        THEN 1
      ELSE 0
    END AS custo_image_icon,
    CASE WHEN
        (perso_custo_flag = 1)
        AND LOWER(attribute_name) LIKE "%scent%" 
        AND (LOWER(attribute_name) NOT LIKE "%crescent%" AND (LOWER(attribute_name) NOT LIKE "iridescent"))
        OR (lower(title) like '% scent%' AND lower(title) not like "%unscented%") 
        THEN 1
      ELSE 0
    END AS custo_scent,
    CASE WHEN
        (perso_custo_flag = 1 AND attribute_personalizable = 0)
        AND trim(lower(attribute_name)) like "text"
        THEN 1
      ELSE 0
    END AS custo_text,
    CASE WHEN
        (perso_custo_flag = 1)
        AND lower(attribute_name) like "%number%" 
        AND lower(attribute_name) not like "%number of%" AND lower(attribute_name) not like "%design%"
        THEN 1
      ELSE 0
    END AS custo_number,
    CASE WHEN
        (perso_custo_flag = 1)
        AND REGEXP_CONTAINS(LOWER(title), r' audio|soundwave|sound wave')
        THEN 1
      ELSE 0
    END AS perso_audio,
    CASE WHEN
        (perso_custo_flag = 1 AND attribute_personalizable = 1)
        AND REGEXP_CONTAINS(LOWER(instructions), r'date')
        THEN 1
      ELSE 0
    END AS perso_date,
    CASE WHEN
        (perso_custo_flag = 1 AND attribute_personalizable = 1)
        AND REGEXP_CONTAINS(LOWER(instructions), r'text|phrase')
        THEN 1
      ELSE 0
    END AS perso_text,
FROM pero_custo_label
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
FROM perso_custo_detail_label
GROUP BY 1,2,3,4,5,6
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.knowledge_base.perso_custo_listing_label_level` AS (
SELECT current_date() AS run_date,'custo_birth_flower' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_birth_flower = 1
UNION ALL
SELECT current_date() AS run_date,'custo_birth_stone' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_birth_stone = 1
UNION ALL
SELECT current_date() AS run_date,'custo_zodiac_sign' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_zodiac_sign = 1
UNION ALL
SELECT current_date() AS run_date,'custo_location' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_location = 1
UNION ALL
SELECT current_date() AS run_date,'custo_initial' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_initial = 1
UNION ALL
SELECT current_date() AS run_date,'custo_moon_phase' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_moon_phase = 1
UNION ALL
SELECT current_date() AS run_date,'custo_font' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_font = 1
UNION ALL
SELECT current_date() AS run_date,'custo_shape' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_shape = 1
UNION ALL
SELECT current_date() AS run_date,'custo_image_icon' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_image_icon = 1
UNION ALL
SELECT current_date() AS run_date,'custo_pattern' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_pattern = 1
UNION ALL
SELECT current_date() AS run_date,'custo_scent' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_scent = 1
UNION ALL
SELECT current_date() AS run_date,'custo_text' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_text = 1
UNION ALL
SELECT current_date() AS run_date,'custo_number' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_number = 1
UNION ALL
SELECT current_date() AS run_date,'perso_audio' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_audio = 1
UNION ALL
SELECT current_date() AS run_date,'perso_date' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_date = 1
UNION ALL
SELECT current_date() AS run_date,'perso_text' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_text = 1
UNION ALL
SELECT current_date() AS run_date,'perso_address' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_address = 1
UNION ALL
SELECT current_date() AS run_date,'perso_name' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_name = 1
UNION ALL
SELECT current_date() AS run_date,'perso_initial' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_initial = 1
UNION ALL
SELECT current_date() AS run_date,'perso_image' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE perso_image = 1
UNION ALL
SELECT current_date() AS run_date,'no_label' as perso_custo_label, listing_id, shop_id, title, variation_count, is_personalizable
FROM `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` WHERE custo_birth_flower+custo_birth_stone+custo_zodiac_sign+custo_location+custo_initial+custo_moon_phase+custo_font+custo_shape+custo_image_icon+custo_pattern+custo_scent+custo_text+custo_number+perso_audio+perso_date+perso_text+perso_address+perso_name+perso_initial+perso_image = 0
)
;

