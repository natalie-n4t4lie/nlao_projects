-- owner: nlao@etsy.com
-- dependency: 
    -- `etsy-data-warehouse-prod.listing_mart.listings_vw`
    -- `etsy-data-warehouse-prod.listing_mart.listing_attributes`
    -- `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations`
    -- `etsy-data-warehouse-prod.etsy_shard.listing_personalization_current_revisions`
 
-- Description: this table is intended to label personalized/customized listing using string manipulation
BEGIN 
CREATE TEMP TABLE perso_custo_listings AS (
-- Filter perso/custo keywords
with listings as (
SELECT
  listing_id,
  shop_id,
  title,
  variation_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_vw`
WHERE (LOWER(title) LIKE '%personali%' AND LOWER(title) NOT LIKE '%personality%')
   OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
   OR REGEXP_CONTAINS(LOWER(title),r'monogram|made to order|made-to-order')
)
-- Filter on varation_count and is_personalizable: is_personalizable = 1 or (variation_count=1/2)
SELECT 
 ll.listing_id,
 ll.shop_id,
 ll.title,
 ll.variation_count,
 la.is_personalizable
FROM listings ll 
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` la USING (listing_id)
WHERE 
  is_personalizable = 1 OR variation_count in (1,2)
)
;
 
-- extract personalization instruction field from each listings
CREATE TEMP TABLE personalization_instruction as (
SELECT 
  cr.listing_id,
  tr.instructions
FROM `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` tr
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_personalization_current_revisions` cr
ON tr.shop_id = cr.shop_id AND tr.listing_personalization_revision_id = cr.listing_personalization_revision_id
WHERE 
  cr.listing_id in (SELECT LISTING_ID FROM perso_custo_listings)
);
 
 
CREATE OR REPLACE TABLE `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` AS (
 
-- create cte table for each perso/custo label
WITH custo_birth_flower as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Birth Flower' as concept_label,
  'Customized Birth Flower' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(title), r'birth flower|birthflower|birth month flower|birthmonth flower') 
     AND (REGEXP_CONTAINS(LOWER(attribute_name), r'month|birth month|flower month')
     OR REGEXP_CONTAINS(LOWER(instructions), r'month|birth month|flower')
     )
  )
  OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth flower|birthflower|birth month flower|birthmonth flower')
  OR REGEXP_CONTAINS(LOWER(instructions), r'birth flower|birthflower|birth month flower|birthmonth flower')
),
custo_birth_stone as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Birth Stone' as concept_label,
  'Customized Birth Stone' as full_label  
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(title), r'birth stone|birthstone|birth month stone|birthmonth stone') 
     AND (REGEXP_CONTAINS(LOWER(attribute_name), r'month|birth month|stone') 
          OR REGEXP_CONTAINS(LOWER(instructions), r'month|birth month|stone')
          )
  )
  OR REGEXP_CONTAINS(LOWER(attribute_name), r'birth stone|birthstone|birth month stone|birthmonth stone')
  OR REGEXP_CONTAINS(LOWER(instructions), r'birth stone|birthstone|birth month stone|birthmonth stone')
),
custo_zodiac_sign as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Zodiac' as concept_label,
  'Customized Zodiac' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva1 USING (listing_id)
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva2 
    ON lva1.listing_id = lva2.listing_id AND lva1.attribute_name = lva2.attribute_name
WHERE 
  (REGEXP_CONTAINS(LOWER(title), r'zodiac|astrology|horoscope|star sign') 
    AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'birthmonth|month|birth month'))
  OR REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'zodiac|astrology|horoscope|star sign')
  OR (REGEXP_CONTAINS(LOWER(lva1.attribute_value), r'sagittarius') AND REGEXP_CONTAINS(LOWER(lva2.attribute_value), r'taurus'))
),
custo_location as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Location' as concept_label,
  'Customized Location' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(title), r'state|province|country|region') AND NOT REGEXP_CONTAINS(LOWER(title), r'estate|statement|country style|farmhouse|style'))
  OR (REGEXP_CONTAINS(LOWER(attribute_name), r'location|state|province|country|region') AND NOT REGEXP_CONTAINS(LOWER(attribute_name), r'print location|printing location|embroidery location'))
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
  OR REGEXP_CONTAINS(LOWER(instructions), r'province|country|region')
  OR (LOWER(instructions) LIKE "city %" 
    OR LOWER(instructions) LIKE "city%" 
    OR LOWER(instructions) LIKE "% city" 
    OR LOWER(instructions) LIKE "% city %" 
    OR LOWER(instructions) LIKE "city")  
),
custo_initial as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Initial' as concept_label,
  'Customized Initial' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character|monogram|engrav|personali|embroid|custom') AND REGEXP_CONTAINS(LOWER(attribute_name), r'color|colour|couleur|größe|farbe') AND is_personalizable=0)
  OR lower(attribute_value) like "% z %"
  OR lower(attribute_value) like "z %"
  OR lower(attribute_value) like "% z"
  OR lower(attribute_value) like "z"
),
custo_moon_phase as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Moon Phase' as concept_label,
  'Customized Moon Phase' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(title), r'moon phase|moonphase|birthmoon|birth moon') AND REGEXP_CONTAINS(LOWER(title), r'birthday|anniversary'))
  OR REGEXP_CONTAINS(LOWER(attribute_name), r'moon phase|moonphase')
  OR LOWER(attribute_value) LIKE "%waxing gibbous%"
), 
custo_font as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Font' as concept_label,
  'Customized Font' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  (REGEXP_CONTAINS(LOWER(attribute_name), r'font|font style|schriftart|letter style') AND NOT REGEXP_CONTAINS(LOWER(attribute_name), r'font color|color of font'))
  OR REGEXP_CONTAINS(LOWER(attribute_value), r'arial|times new roman|comic sans')
),
custo_shape as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Shape' as concept_label,
  'Customized Shape' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  LOWER(attribute_name) LIKE "%shape%"
),
custo_image_icon as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Image' as concept_label,
  'Customized Image' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  LOWER(attribute_name) LIKE "image" 
  OR LOWER(attribute_name) LIKE "photo"
  OR LOWER(attribute_name) LIKE "%icon%"
  OR (LOWER(attribute_name) LIKE "%stamp%" AND NOT REGEXP_CONTAINS(LOWER(attribute_name), r'number|size|color|type|style|case'))
),
custo_pattern as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Pattern' as concept_label,
  'Customized Pattern' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  REGEXP_CONTAINS(LOWER(attribute_name), r'pattern|fabric')
),
custo_scent as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Scent' as concept_label,
  'Customized Scent' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  LOWER(attribute_name) LIKE "%scent%" AND (LOWER(attribute_name) NOT LIKE "%crescent%" AND LOWER(attribute_name) NOT LIKE "%iridescent%")
  OR LOWER(attribute_name) LIKE "%flavor%"
  OR (LOWER(title) LIKE '% scent%' AND LOWER(title) NOT LIKE "%unscented%")
),
custo_text as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Text' as concept_label,
  'Customized Text' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  is_personalizable = 0
  AND (LOWER(attribute_name) LIKE "text" 
    OR LOWER(attribute_name) LIKE "hebrew text" 
    OR LOWER(attribute_name) LIKE "choisir votre texte" 
    OR LOWER(attribute_name) LIKE "saying" 
    OR LOWER(attribute_name) LIKE "%title%" 
    OR LOWER(attribute_name) LIKE "%wording%"
    OR LOWER(attribute_name) LIKE "%word%" 
    OR LOWER(attribute_name) LIKE "text selection" 
    OR LOWER(attribute_name) LIKE "name"
    OR LOWER(attribute_name) LIKE "names")
  AND LOWER(attribute_name) NOT LIKE "%color%"
),
custo_number as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Number' as concept_label,
  'Customized Number' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  (LOWER(attribute_name) LIKE "%number%" AND NOT REGEXP_CONTAINS(LOWER(attribute_name), r'number of|number per|font number|design number|style number')) 
  OR LOWER(attribute_name) LIKE "%year%" 
  OR LOWER(attribute_name) LIKE " age%" 
  OR LOWER(attribute_name) LIKE "age" 
  OR LOWER(attribute_name) LIKE "% age" 
  OR LOWER(attribute_name) LIKE "% age %" 
),
perso_audio as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Audio' as concept_label,
  'Personalized Audio' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  REGEXP_CONTAINS(LOWER(title), r'soundwave|sound wave')
),
perso_date as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Date' as concept_label,
  'Personalized Date' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  is_personalizable = 1
  AND (REGEXP_CONTAINS(LOWER(instructions), r'date|birthday|month|year|datum') 
    OR REGEXP_CONTAINS(LOWER(attribute_name), r'year'))
  AND NOT REGEXP_CONTAINS(LOWER(instructions), r'happy birthday|delivery date')
),
perso_text as (
SELECT 
 distinct l.*,
 'Personalized' as perso_custo_label,
 'Text' as concept_label,
 'Personalized Text' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  is_personalizable = 1
  AND (REGEXP_CONTAINS(LOWER(instructions), r'text|phrase|wording|word|saying|message|letter|lyric|written|font|print|quote|title|carve|writing')
    OR LOWER(attribute_name) LIKE "text" 
    OR LOWER(attribute_name) LIKE "choisir votre texte" 
    OR LOWER(attribute_name) LIKE "saying" 
    OR LOWER(attribute_name) LIKE "%title%" 
    OR LOWER(attribute_name) LIKE "%wording%" 
    OR LOWER(attribute_name) LIKE "%word%" 
    OR LOWER(attribute_name) LIKE "text selection" 
    OR LOWER(attribute_name) LIKE "name"
    OR LOWER(attribute_name) LIKE "names")
),
perso_address as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Address' as concept_label,
  'Personalized Address' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  is_personalizable = 1
  AND REGEXP_CONTAINS(LOWER(instructions), r'address|latitude') 
  AND NOT REGEXP_CONTAINS(LOWER(instructions), r'email address|e-mail address|shipping address|living address') 
),
perso_name as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Name' as concept_label,
  'Personalized Name' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  is_personalizable = 1 
  AND (REGEXP_CONTAINS(LOWER(instructions), r'name|character|surname|prénom|engrav|personali|embriod|custom|monogram') 
    OR REGEXP_CONTAINS(LOWER(attribute_name), r'engrav|personali|embriod|custom|monogram'))
),
perso_initial as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Initial' as concept_label,
  'Personalized Initial' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
LEFT JOIN personalization_instruction USING (listing_id)
WHERE 
  is_personalizable = 1
  AND (REGEXP_CONTAINS(LOWER(instructions), r'initial')
  OR REGEXP_CONTAINS(LOWER(attribute_name), r'alphabet|letter|initial|character|monogram|engrav|personali|embroid|custom'))
   
), 
perso_image as (
SELECT 
  distinct l.*,
  'Personalized' as perso_custo_label,
  'Image' as concept_label,
  'Personalized Image' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva USING (listing_id)
WHERE 
  REGEXP_CONTAINS(LOWER(title), r'portrait|logo')
), 
custo_mix_and_match as (
SELECT 
  distinct l.*,
  'Customized' as perso_custo_label,
  'Mix and Match' as concept_label,
  'Customized Mix and Match' as full_label
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva1 on l.listing_id=lva1.listing_id
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva2 on l.listing_id=lva2.listing_id AND lva1.attribute_name<lva2.attribute_name
LEFT JOIN personalization_instruction pi on l.listing_id=pi.listing_id
WHERE 
  (variation_count=2 and is_personalizable=0 
  AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material')
  AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'color|colour|couleur|größe|farbe|material'))
  OR 
  (variation_count=1 and is_personalizable=1
  AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material')
  AND REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material'))
  OR 
  (variation_count=2 and is_personalizable=1 
  AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material')
  AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'color|colour|couleur|größe|farbe|material')) 
  OR 
  (variation_count=2 and is_personalizable=1 
  AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material')
  AND REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material'))
  OR 
  (variation_count=2 and is_personalizable=1 
  AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'color|colour|couleur|größe|farbe|material')
  AND REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material')) 
),
temp_label as (
  SELECT current_date() as run_date, * FROM custo_birth_flower
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_birth_stone 
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_zodiac_sign 
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_location    
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_initial 
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_moon_phase  
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_font    
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_shape   
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_image_icon  
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_pattern 
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_scent   
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_text    
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_number  
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_audio   
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_date    
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_text    
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_address 
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_name    
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_initial 
  UNION ALL
  SELECT current_date() as run_date, * FROM perso_image 
  UNION ALL
  SELECT current_date() as run_date, * FROM custo_mix_and_match
),
last_catch_exclusion_variation1 as (
SELECT 
  distinct l.*
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva
  ON l.listing_id = lva.listing_id
LEFT JOIN personalization_instruction pi 
  ON l.listing_id = pi.listing_id
WHERE 
  l.listing_id NOT IN (SELECT listing_id FROM temp_label) 
  AND (
    (variation_count = 0 AND is_personalizable = 1 
        -- exclude listings with personalization instructions that's null or gathers buyer contact info or include color/size keywords
        AND (LENGTH(instructions) <= 2 OR instructions IS NULL OR REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material|set|pacakging|chain|count|pack|box type|charm|insert|box|cost|handing|print|digital|frame|pairs|insert|size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap|contact number|phone number|shipping|delivery|https://')))  
    OR (variation_count IS NULL AND is_personalizable = 1 
        -- exclude listings with personalization instructions that's null or gathers buyer contact info or include color/size keywords
        AND (LENGTH(instructions) <= 2 OR instructions IS NULL OR REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material|set|pacakging|chain|count|pack|box type|charm|insert|box|cost|handing|print|digital|frame|pairs|insert|size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap|contact number|phone number|shipping|delivery|https://')))  
    OR (variation_count = 1 AND is_personalizable = 0 
        -- exclude listings with variation name that includes color/size/quantity/gift box keywords  
        AND (attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva.attribute_name), r'color|colour|couleur|größe|farbe|material|set|pacakging|chain|count|pack|box type|charm|insert|box|cost|handing|print|digital|frame|pairs|insert|size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')))  
    OR (variation_count = 1 AND is_personalizable = 1 
        -- exclude listings that collect buyer contact or null from personalization box AND variation name is null or includes color/size/quantity/gift box keywords
       AND (LENGTH(instructions) <= 2 OR instructions IS NULL OR REGEXP_CONTAINS(LOWER(pi.instructions), r'contact number|phone number|shipping|delivery|https://')) 
       AND (lva.attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva.attribute_name), r'color|colour|couleur|größe|farbe|material|set|pacakging|chain|count|pack|box type|charm|insert|box|cost|handing|print|digital|frame|pairs|insert|size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')))
    OR (variation_count = 1 and is_personalizable = 1 
       -- exclude size & color combiantion
       AND REGEXP_CONTAINS(LOWER(pi.instructions), r'color|colour|couleur|größe|farbe|material')
       AND REGEXP_CONTAINS(LOWER(lva.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap'))
  OR (variation_count = 1 and is_personalizable = 1 
       -- exclude size & color combiantion
       AND REGEXP_CONTAINS(LOWER(lva.attribute_name), r'color|colour|couleur|größe|farbe|material')
       AND REGEXP_CONTAINS(LOWER(pi.instructions), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')))
), 
last_catch_exclusion_variation2 as (
SELECT 
  distinct l.*
FROM perso_custo_listings l
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva1 
  ON l.listing_id = lva1.listing_id
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva2 
  ON l.listing_id = lva2.listing_id AND lva1.attribute_name < lva2.attribute_name
LEFT JOIN personalization_instruction pi 
  ON l.listing_id = pi.listing_id
WHERE l.listing_id NOT IN (SELECT listing_id FROM temp_label) 
    AND 
    -- exclude size & color combiantion or variation is null
    (variation_count = 2 AND is_personalizable = 0
      AND (lva1.attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material')) 
        AND (lva2.attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap'))
    )
    OR (variation_count = 2 AND is_personalizable = 0
      AND (lva2.attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'color|colour|couleur|größe|farbe|material')) 
       AND (lva1.attribute_name IS NULL OR REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap'))
    )
    OR (variation_count = 2 AND is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(instructions), r'color|colour|couleur|größe|farbe|material') 
      AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')
    )
    OR (variation_count = 2 AND is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(instructions), r'color|colour|couleur|größe|farbe|material') 
      AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')
    )
    OR (variation_count = 2 AND is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'color|colour|couleur|größe|farbe|material') 
      AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')
    )
    OR (variation_count = 2 AND is_personalizable = 1
      AND REGEXP_CONTAINS(LOWER(lva2.attribute_name), r'color|colour|couleur|größe|farbe|material') 
      AND REGEXP_CONTAINS(LOWER(lva1.attribute_name), r'size|sizing|größe|diameter|length|länge|width|dimension|height|weight|deepth|depth|sq/ft|sqft|volume|capacity|taille|measurement|number of|quanti|quanity|amount|qty|pieces|pcs|count|how many|model|modèle|device|phone type|gift box|gift wrap')
    )
 
),
no_label as (
SELECT 
  distinct l.*,
  'No Label' as perso_custo_label,
  'No Label' as concept_label,
  'No Label' as full_label
FROM perso_custo_listings l
WHERE listing_id NOT IN (SELECT listing_id FROM temp_label) 
  AND listing_id NOT IN (SELECT listing_id FROM last_catch_exclusion_variation1)
  AND listing_id NOT IN (SELECT listing_id FROM last_catch_exclusion_variation2)
)
 
SELECT
  * 
FROM temp_label
UNION ALL
select 
current_date() as run_date,
*
from no_label
);
 
END;
 
 
