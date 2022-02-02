CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` AS (

-- 1) extract perso/custo keywords from title
-- 2) get number of variation
-- 3) is_personalizable flag
-- 4) if listing has perso/custo keywords AND (number of variation >=1 OR is_personalizable =1)
WITH
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
    lpft.instructions as personalization_instruction
  FROM
    `etsy-data-warehouse-prod.listing_mart.listing_vw` l
  LEFT JOIN 
    `etsy-data-warehouse-prod.listing_mart.listing_attributes` la
  ON l.listing_id = la.listing_id
  LEFT JOIN 
    `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva
  ON l.listing_id = lva.listing_id
  LEFT JOIN 
    `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` lpft
  ON l.listing_id = lpft.listing_id
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
        AND (LOWER(attribute_name) NOT LIKE "%crescent%" AND (LOWER(attribute_name) NOT LIKE "%iridescent%"))
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

SELECT 
  listing_id,
  shop_id,
  title,
  variation_count,
  title_text_custom,
  attribute_personalizable,
  perso_custo_flag, 
  custo_birth_flower,
  custo_birth_stone,  
  custo_zodiac_sign,
  custo_location,
  custo_initial,
  custo_moon_phase,
  custo_font,
  custo_shape,
  custo_image_icon,
  custo_scent,
  custo_text,
  custo_number,
  perso_audio,
FROM perso_custo_detail_label
)




