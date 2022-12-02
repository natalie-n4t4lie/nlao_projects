-- LIST OF H&L DIMENSION ATTRIBUTES
SELECT DISTINCT
  a.attribute_id,
  a.name,
  a.user_input_validator
FROM `etsy-data-warehouse-prod.structured_data.attributes` a
JOIN `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` ca
  ON a.attribute_id = ca.attribute_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON ca.taxonomy_id = t.taxonomy_id
WHERE user_input_validator like '%"type":"measurement"%' AND full_path LIKE "home_and_living%"
;

-- DIMESNION ATTRIBUTE ELIGIBLE / ADOPTED / ADOPTION RATE
WITH candidate_listing_w_measurement_attribute AS (
SELECT 
  listing_id
  ,is_set
FROM `etsy-data-warehouse-prod.structured_data.attributes` a
JOIN `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` ca
  ON a.attribute_id = ca.attribute_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON ca.taxonomy_id = t.taxonomy_id
WHERE user_input_validator like '%"type":"measurement"%' 
-- AND full_path LIKE "home_and_living%"
)
SELECT
  COUNT(DISTINCT listing_id) AS listing_count
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM candidate_listing_w_measurement_attribute) THEN listing_id ELSE NULL END) AS measurement_attribute_available
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM candidate_listing_w_measurement_attribute WHERE is_set = 1) THEN listing_id ELSE NULL END) AS measurement_attribute_adopt
  ,SUM(past_year_gms) AS listing_gms
  ,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM candidate_listing_w_measurement_attribute) THEN past_year_gms ELSE NULL END) AS measurement_attribute_available_gms
  ,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM candidate_listing_w_measurement_attribute WHERE is_set = 1) THEN past_year_gms ELSE NULL END) AS measurement_attribute_adopt_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
WHERE alb.top_category = 'home_and_living'
;

SELECT 17653688/25162715; --68.5%
SELECT 7490522/25638836;--29.2%

SELECT 1726093463.04 / 2622789160.97;--65.8%
SELECT 710360218.21 / 2622789160.97;--27.1%

--TAXONOMY ID THAT ARE NOT ELIGIBLE FOR DIMENSION ATTRIBUTES
WITH candidate_listing_w_measurement_attribute AS (
SELECT 
  listing_id
FROM `etsy-data-warehouse-prod.structured_data.attributes` a
JOIN `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` ca
  ON a.attribute_id = ca.attribute_id
WHERE user_input_validator like '%"type":"measurement"%'
)
SELECT
  t.full_path
  ,t.taxonomy_id
  ,t.is_buyer
  ,t.is_seller
  ,t.is_active
  ,COUNT(DISTINCT listing_id) AS listing_count
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON alb.taxonomy_id = t.taxonomy_id
WHERE alb.top_category = 'home_and_living' AND listing_id NOT IN (SELECT listing_id FROM candidate_listing_w_measurement_attribute)
GROUP BY 1,2,3,4,5
ORDER BY 6 DESC
;

-- WHAT TAXO LISTING IS ELIGIBLE FOR "DIAMETER"
SELECT 
  DISTINCT t.full_path
  ,t.taxonomy_id
  ,t.is_buyer
  ,t.is_seller
  ,t.is_active
FROM `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` ca
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON ca.taxonomy_id = t.taxonomy_id
WHERE attribute_id = 53 AND full_path like 'home_and_living%'
;

-----------------------------------------
---- NON-STRUCTURE DATA STRING MATCH ----
-----------------------------------------
-- GET FULL DESCRIPTIONS
BEGIN
create temporary table listing_table_titles as (
select listing_id,
        regexp_replace(replace(title,'&#39;','\''),'&quot;|&Quot;','\"') as listing_table_title,
        regexp_replace(replace(description,'&#39;','\''),'&quot;|&Quot;','\"') as listing_table_description
from `etsy-data-warehouse-prod.etsy_shard.listings`);

create temporary table listing_translated_titles as (
select listing_id,
        regexp_replace(replace(title,'&#39;','\''),'&quot;|&Quot;','\"') as translated_title,
        regexp_replace(replace(description,'&#39;','\''),'&quot;|&Quot;','\"')  as translated_description
from `etsy-data-warehouse-prod.etsy_shard.listing_translations`
where language = 5);

create or replace table `etsy-data-warehouse-dev.nlao.listing_titles_description` 
cluster by listing_id as (
    select ll.listing_id,
        listing_table_title,
        listing_table_description,
        case when l.listing_table_title = '' then
            case when lt.translated_title <> '' then lt.translated_title
            else l.listing_table_title end 
        else l.listing_table_title end as title,
        case when l.listing_table_description = '' then
            case when lt.translated_description <> '' then lt.translated_description
            else l.listing_table_description end 
        else l.listing_table_description end as description,
        case when l.listing_table_title = '' and lt.translated_title <> '' then 1 else 0 end as is_translated
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` ll
  join listing_table_titles l
  using (listing_id)
  left join listing_translated_titles lt 
  using (listing_id)
  WHERE ll.top_category = 'home_and_living'
);
END;

--Resource: https://www.regextester.com/116235
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.dimension_flag` AS (
WITH listing_w_measurement_attribute AS (
SELECT 
  listing_id
FROM `etsy-data-warehouse-prod.structured_data.attributes` a
JOIN `etsy-data-warehouse-prod.rollups.active_listings_w_candidate_attributes` ca
  ON a.attribute_id = ca.attribute_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON ca.taxonomy_id = t.taxonomy_id
WHERE user_input_validator like '%"type":"measurement"%' 
      AND is_set = 1
      -- AND full_path LIKE "home_and_living%" 
)
,listing_w_size_scale_chart AS (
SELECT
    listing_id
FROM `etsy-data-warehouse-prod.computer_vision.listing_image_categorization` li
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE li.size_and_scale >= 0.6
    OR li.size_chart >= 0.6
)
,listing_w_size_custom_variation AS (
SELECT
  DISTINCT listing_id
FROM `etsy-data-warehouse-prod.rollups.listing_variations_extended`
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
WHERE top_category = 'home_and_living'
      AND REGEXP_CONTAINS(custom_property_name,r'(?i)size|sizing|diameter|length|width|dimension|height|deepth|depth|thickness|volume|capacity|measurement|größe|dimensionierung|durchmesser|länge|breite|abmessung|höhe|dicke|tiefe|volumen|fassungsvermögen|messung|taille|dimensionnement|diamètre|longueur|largeur|la dimension|la taille|profondeur|profond|épaisseur|tome|mesure')
)
SELECT
  listing_id
  ,CASE WHEN (
      listing_id IN (SELECT listing_id FROM listing_w_measurement_attribute)  
      ) THEN 1 ELSE 0 END AS attribute_match
  ,CASE WHEN (
      listing_id IN (SELECT listing_id FROM listing_w_size_scale_chart)  
      ) THEN 1 ELSE 0 END AS image_match
  ,CASE WHEN (
      listing_id IN (SELECT listing_id FROM listing_w_size_custom_variation)  
      ) THEN 1 ELSE 0 END AS variation_match
  ,CASE WHEN (
      REGEXP_CONTAINS(description, r'(\d+(\.\d+|)\s?[x|*]\s?\d+(\.\d+|)(\s?[x|*]\s?\d[x|*](\.?\d+|))?)') 
      OR REGEXP_CONTAINS(description, r'\s*\d+\s*(m|"|”|mm|cm|km|ft|in|sqft|inch|foot|yard|mile|sq/ft|mile|miles|metre|meter|inches|metres|meters|kilometer|millimeter|millimetre|centimeter|centimetre|kilo meter|square metre|square meter)')
      ) THEN 1 ELSE 0 END AS description_match
  ,CASE WHEN (
      REGEXP_CONTAINS(title, r'(\d+(\.\d+|)\s?[x|*]\s?\d+(\.\d+|)(\s?[x|*]\s?\d[x|*](\.?\d+|))?)') 
      OR REGEXP_CONTAINS(title, r'\s*\d+\s*(m|"|”|mm|cm|km|ft|in|sqft|inch|foot|yard|mile|sq/ft|mile|miles|metre|meter|inches|metres|meters|kilometer|millimeter|millimetre|centimeter|centimetre|kilo meter|square metre|square meter)')
      ) THEN 1 ELSE 0 END AS title_match
FROM `etsy-data-warehouse-dev.nlao.listing_titles_description` 
)
;

SELECT COUNT(CASE WHEN attribute_match = 1 THEN listing_id ELSE NULL END)/COUNT(LISTING_ID)
FROM `etsy-data-warehouse-dev.nlao.dimension_flag`
;

-- listing coverage and gms coverage
SELECT
df.description_match
,df.title_match
,df.image_match
,df.variation_match
,df.attribute_match
,COUNT(alb.listing_id) AS listing_count
,SUM(alb.past_year_gms) AS gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-dev.nlao.dimension_flag` df USING (listing_id)
GROUP BY 1,2,3,4,5
;

-- listing view coverage
SELECT
df.description_match
,df.title_match
,df.image_match
,df.variation_match
,df.attribute_match
,COUNT(*) AS listing_view_count
FROM `etsy-data-warehouse-prod.analytics.listing_views` lv
JOIN `etsy-data-warehouse-dev.nlao.dimension_flag` df USING (listing_id)
WHERE _date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 31 DAY) AND CURRENT_DATE()
GROUP BY 1,2,3,4,5
;

-- home and living search query coverage
WITH
  hl_search AS (
  SELECT
    query
  FROM
    `etsy-data-warehouse-prod.search.query_sessions_new` qs -- WEB ONLY table
  JOIN
    `etsy-data-warehouse-prod.structured_data.taxonomy_latest` t
  ON
    t.taxonomy_id = qs.classified_taxonomy_id
  WHERE
    qs._date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND SPLIT(t.full_path,'.')[SAFE_OFFSET(0)] = 'home_and_living' 
  GROUP BY 1
)
SELECT
  CASE
    WHEN df.description_match + df.title_match + df.image_match + df.variation_match + df.attribute_match > 0 THEN 1
  ELSE
  0
END
  AS dimension_info_flag
  ,SUM(i.impressions) AS impressions
FROM `etsy-data-warehouse-dev.nlao.dimension_flag` df
JOIN `etsy-data-warehouse-prod.search.visit_level_listing_impressions` i
  ON df.listing_id = i.listing_id
JOIN  hl_search hls
  ON hls.query = i.query
WHERE
    i._date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
;

-- listing coverage by subcategory
SELECT
  CASE
    WHEN df.description_match + df.title_match + df.image_match + df.variation_match + df.attribute_match > 0 THEN 1
  ELSE
  0
END
  AS dimension_info_flag,
  t.full_path,
  COUNT(alb.listing_id) AS listing_count,
  SUM(alb.past_year_gms) AS gms
FROM
  `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN
  `etsy-data-warehouse-prod.structured_data.taxonomy` t
ON
  alb.taxonomy_id = t.taxonomy_id
JOIN
  `etsy-data-warehouse-dev.nlao.dimension_flag` df
ON
  alb.listing_id = df.listing_id
WHERE full_path LIKE "home_and_living%"
GROUP BY
  1,
  2 ;

--SPOT CHECKING TYPE I AND TYPE II ERROR
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.dimension_flag`
WHERE image_match = 1
limit 50
;

SELECT
*
FROM `etsy-data-warehouse-dev.nlao.dimension_flag`
WHERE image_match = 0
limit 50
;
