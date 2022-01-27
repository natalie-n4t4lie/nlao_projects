-- birth stone/flower
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.perso_custo_listing_label` AS (
WITH
  pero_custo_label AS (
  SELECT
    lt.listing_id,
    shop_id,
    title,
    description,
    CASE WHEN 
        (LOWER(title) LIKE '%personali%' AND title NOT LIKE '%personality%') 
        OR LOWER(title) LIKE '%monogram%' 
        OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%') 
        OR LOWER(title) LIKE '%made to order%' OR LOWER(title) LIKE '%made-to-order%' 
        THEN 1
      ELSE 0
    END AS title_text_custom,
    CASE WHEN 
        (LOWER(description) LIKE '%personali%' AND LOWER(description) NOT LIKE '%personality%')
        OR LOWER(description) LIKE '%monogram%'
        OR (LOWER(description) LIKE '%custom%' AND LOWER(description) NOT LIKE '%customer%')
        OR LOWER(description) LIKE '%made to order%' OR LOWER(description) LIKE '%made-to-order%' 
        THEN 1
      ELSE 0
    END AS description_text_custom,
    is_personalizable AS attribute_personalizable,
    lva.attribute_name,
    lva.attribute_value
  FROM
    `etsy-data-warehouse-prod.listing_mart.listing_titles` lt
  INNER JOIN
    `etsy-data-warehouse-prod.listing_mart.listing_attributes` la
  ON
    lt.listing_id = la.listing_id
  INNER JOIN
    `etsy-data-warehouse-prod.listing_mart.listings` l
  ON
    lt.listing_id = l.listing_id
  LEFT JOIN 
    `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva
  ON lva.listing_id = lt.listing_id
)

SELECT
  listing_id,
  shop_id,
  title,
  description,
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(title), r'birth flower|birthflower|birth month flower|birthmonth flower') OR
        REGEXP_CONTAINS(LOWER(attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower')
      THEN 1
      ELSE 0
    END AS birth_flower,
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(title), r'birth stone|birthstone|birth month stone|birthmonth stone') OR
        REGEXP_CONTAINS(LOWER(attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone')
      THEN 1
      ELSE 0
    END AS birth_stone,  
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(title), r'zodiac|astrology|constellation|star sign') OR
        REGEXP_CONTAINS(LOWER(attribute_name), r'zodiac|astrology|constellation|star sign') OR
        (REGEXP_CONTAINS(LOWER(attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(attribute_value), r'taurus'))
      THEN 1
      ELSE 0
    END AS zodiac_sign,
  CASE WHEN 
       (title_text_custom = 1 OR description_text_custom = 1) AND
       ((REGEXP_CONTAINS(LOWER(title), r'state|country|region') OR REGEXP_CONTAINS(LOWER(attribute_name), r'state|country|region')) OR
       (LOWER(title) LIKE "city %" OR LOWER(title) LIKE "city%" OR LOWER(title) LIKE "% city" OR LOWER(title) LIKE "% city %" OR LOWER(title) LIKE "city") OR
       (LOWER(attribute_name) LIKE "city %" OR LOWER(attribute_name) LIKE "city%" OR LOWER(attribute_name) LIKE "% city" OR LOWER(attribute_name) LIKE "% city %" OR LOWER(attribute_name) LIKE "city") OR
       (LOWER(attribute_name) LIKE "state %" OR LOWER(attribute_name) LIKE "state%" OR LOWER(attribute_name) LIKE "% state" OR LOWER(attribute_name) LIKE "% state %" OR LOWER(attribute_name) LIKE "state") OR
       (LOWER(attribute_name) LIKE "country %" OR LOWER(attribute_name) LIKE "country%" OR LOWER(attribute_name) LIKE "% country" OR LOWER(attribute_name) LIKE "% country %" OR LOWER(attribute_name) LIKE "country") OR
       (LOWER(attribute_name) LIKE "region %" OR LOWER(attribute_name) LIKE "region%" OR LOWER(attribute_name) LIKE "% region" OR LOWER(attribute_name) LIKE "% region %" OR LOWER(attribute_name) LIKE "region"))
      THEN 1
      ELSE 0
    END AS location,
  CASE WHEN
        (title_text_custom = 1 OR description_text_custom = 1) AND
        (REGEXP_CONTAINS(LOWER(title), r'alphabet|letter|initial|character') OR
        REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character')) 
        THEN 1
      ELSE 0
    END AS initial,
  CASE WHEN
        (title_text_custom = 1 OR description_text_custom = 1) AND
        (REGEXP_CONTAINS(LOWER(title), r'moon phase') OR REGEXP_CONTAINS(LOWER(title), r'moonphase') OR
        REGEXP_CONTAINS(LOWER(attribute_name), r'moon phase') OR REGEXP_CONTAINS(LOWER(attribute_name), r'moonphase') OR 
        LOWER(attribute_value) LIKE "%waxing gibbous%")
        THEN 1
      ELSE 0
    END AS moon_phase,
  CASE WHEN
        (title_text_custom = 1 OR description_text_custom = 1) AND
        ((REGEXP_CONTAINS(LOWER(attribute_value), r'font|schriftart') AND LOWER(attribute_name) NOT LIKE "%color%" AND LOWER(attribute_name) NOT LIKE "%colour%") OR 
        (REGEXP_CONTAINS(LOWER(attribute_value), r'arial|times new roman|comic sans')))
        THEN 1
      ELSE 0
    END AS font,
  CASE WHEN
        (title_text_custom = 1 OR description_text_custom = 1) AND
        (LOWER(attribute_name) LIKE "%shape%")
        THEN 1
      ELSE 0
    END AS shape,
   CASE WHEN
        (title_text_custom = 1 OR description_text_custom = 1) AND
        (LOWER(attribute_name) LIKE "image" OR LOWER(attribute_name) LIKE "%icon%")
        THEN 1
      ELSE 0
    END AS image_icon    
FROM pero_custo_label
)





