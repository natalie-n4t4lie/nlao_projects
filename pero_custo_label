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
        OR LOWER(title) LIKE '%handmade%' OR LOWER(title) LIKE '%hand-made%' OR LOWER(title) LIKE '%hand made%'
        THEN 1
      ELSE 0
    END AS title_text_custom,
    CASE WHEN 
        (LOWER(description) LIKE '%personali%' AND LOWER(description) NOT LIKE '%personality%')
        OR LOWER(description) LIKE '%monogram%'
        OR (LOWER(description) LIKE '%custom%' AND LOWER(description) NOT LIKE '%customer%')
        OR LOWER(description) LIKE '%made to order%' OR LOWER(description) LIKE '%made-to-order%'
        OR LOWER(description) LIKE '%handmade%' OR LOWER(description) LIKE '%hand-made%' OR LOWER(description) LIKE '%hand made%' 
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
  current_date() as run_date,
  pc.listing_id,
  pc.shop_id,
  pc.title,
  pc.description,
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(pc.title), r'birth flower|birthflower|birth month flower|birthmonth flower') OR
        REGEXP_CONTAINS(LOWER(pc.attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower')
      THEN 1
      ELSE 0
    END AS birth_flower,
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(pc.title), r'birth stone|birthstone|birth month stone|birthmonth stone') OR
        REGEXP_CONTAINS(LOWER(pc.attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone')
      THEN 1
      ELSE 0
    END AS birth_stone,  
  CASE WHEN 
        REGEXP_CONTAINS(LOWER(pc.title), r'zodiac|astrology|constellation|star sign') OR
        REGEXP_CONTAINS(LOWER(pc.attribute_name), r'zodiac|astrology|constellation|star sign') OR
        (REGEXP_CONTAINS(LOWER(pc.attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(pc.attribute_value), r'taurus'))
      THEN 1
      ELSE 0
    END AS zodiac_sign,
  CASE WHEN 
       (pc.title_text_custom = 1 OR pc.description_text_custom = 1) AND
       ((REGEXP_CONTAINS(LOWER(pc.title), r'state|country|region') OR REGEXP_CONTAINS(LOWER(attribute_name), r'state|country|region')) OR
       (LOWER(pc.title) LIKE "city %" OR LOWER(pc.title) LIKE "city%" OR LOWER(pc.title) LIKE "% city" OR LOWER(pc.title) LIKE "% city %" OR LOWER(pc.title) LIKE "city") OR
       (LOWER(pc.attribute_name) LIKE "city %" OR LOWER(pc.attribute_name) LIKE "city%" OR LOWER(pc.attribute_name) LIKE "% city" OR LOWER(pc.attribute_name) LIKE "% city %" OR LOWER(pc.attribute_name) LIKE "city") OR
       (LOWER(pc.attribute_name) LIKE "state %" OR LOWER(pc.attribute_name) LIKE "state%" OR LOWER(pc.attribute_name) LIKE "% state" OR LOWER(pc.attribute_name) LIKE "% state %" OR LOWER(pc.attribute_name) LIKE "state") OR
       (LOWER(pc.attribute_name) LIKE "country %" OR LOWER(pc.attribute_name) LIKE "country%" OR LOWER(pc.attribute_name) LIKE "% country" OR LOWER(pc.attribute_name) LIKE "% country %" OR LOWER(pc.attribute_name) LIKE "country") OR
       (LOWER(pc.attribute_name) LIKE "region %" OR LOWER(pc.attribute_name) LIKE "region%" OR LOWER(pc.attribute_name) LIKE "% region" OR LOWER(pc.attribute_name) LIKE "% region %" OR LOWER(pc.attribute_name) LIKE "region"))
      THEN 1
      ELSE 0
    END AS location,
  CASE WHEN
        (pc.title_text_custom = 1 OR pc.description_text_custom = 1) AND
        (REGEXP_CONTAINS(LOWER(pc.title), r'alphabet|letter|initial') OR
        REGEXP_CONTAINS(LOWER(pc.attribute_name), r'alphabet|letter|initial')) 
        THEN 1
      ELSE 0
    END AS initial,
  CASE WHEN
        (pc.title_text_custom = 1 OR pc.description_text_custom = 1) AND
        (REGEXP_CONTAINS(LOWER(pc.title), r'moon phase|moonphase') OR
        REGEXP_CONTAINS(LOWER(pc.attribute_name), r'moon phase|moonphase') OR 
        LOWER(pc.attribute_value) LIKE "%waxing gibbous%")
        THEN 1
      ELSE 0
    END AS moon_phase,
    lv.is_active,
    lv.top_category,
    lg.past_year_gms
FROM pero_custo_label pc
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` lv USING (listing_id)
JOIN `etsy-data-warehouse-prod.listing_mart.listing_gms` lg USING (listing_id)
)





