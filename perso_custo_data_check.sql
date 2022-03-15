# Label vs No label
SELECT 
COUNT(distinct CASE WHEN perso_custo_label='No Label' THEN listing_id ELSE NULL END) AS no_label_count,
COUNT(distinct CASE WHEN perso_custo_label!='No Label' THEN listing_id ELSE NULL END) AS labeled_count,
COUNT(distinct CASE WHEN perso_custo_label!='No Label' THEN listing_id ELSE NULL END) / COUNT(DISTINCT listing_id) AS percent_listing_with_label
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
;--89%

--listing and gms coverage
WITH perso_custo AS (
SELECT 
distinct listing_id
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
WHERE perso_custo_label!='No Label'
)
SELECT 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM perso_custo) THEN listing_id ELSE NULL END)/COUNT(LISTING_ID) AS listing_coverage,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM perso_custo) THEN past_year_gms ELSE NULL END)/ SUM(past_year_gms) AS gms_coverage
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;--4.3% and 18.3%


# variation and peronalizable combination for no label listing
SELECT 
  variation_count,
  is_personalizable,
  count(listing_id) as attribute_count
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
WHERE perso_custo_label='No Label' 
GROUP BY 1,2
ORDER BY 1 DESC LIMIT 100;

## Top attribute names of those pc listing with no label -- 1 variation AND is personalizable
SELECT 
  lower(attribute_name),
  count(distinct listing_id) as listing_count
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` USING (listing_id)
WHERE perso_custo_label='No Label' AND variation_count=1 AND is_personalizable =0 
GROUP BY 1
ORDER BY 2 DESC LIMIT 100;

## Top attribute names of those pc listing with no label -- two variation not personalizable
SELECT 
  lower(lva1.attribute_name) as variation_1,
  lower(lva2.attribute_name) as variation_2,
  count(distinct lva1.listing_id) as attribute_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva1
JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva2 on lva1.listing_id=lva2.listing_id AND lva1.attribute_name<lva2.attribute_name
WHERE lva1.listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` where perso_custo_label='No Label' AND variation_count=2 AND is_personalizable =0)
GROUP BY 1,2
ORDER BY 3 DESC LIMIT 100;

## Top attribute names of those pc listing with no label -- two variation not personalizable
SELECT 
  lower(lva1.attribute_name) as variation_1,
  lower(lva2.attribute_name) as variation_2,
  count(distinct lva1.listing_id) as attribute_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva1
JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva2 on lva1.listing_id=lva2.listing_id AND lva1.attribute_name<lva2.attribute_name
WHERE lva1.listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` where perso_custo_label='No Label' AND variation_count=2 AND is_personalizable =1)
GROUP BY 1,2
ORDER BY 3 DESC LIMIT 100;


# Personalize enabled or not
SELECT 
is_personalizable,
count(listing_id) as listing_count
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
WHERE perso_custo_label='No Label'
GROUP BY 1
;
-- not personalizable 6016527
-- personalizable 1806645

# Personalization enabled listing instructions
WITH perso_instruction AS (
SELECT 
  cr.listing_id,
  tr.instructions
FROM `etsy-data-warehouse-prod.etsy_shard.listing_personalization_field_translations` tr
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_personalization_current_revisions` cr
ON tr.shop_id = cr.shop_id AND tr.listing_personalization_revision_id = cr.listing_personalization_revision_id
)

SELECT 
instructions,
count(listing_id) as listing_count
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
LEFT JOIN  perso_instruction USING (listing_id)
WHERE perso_custo_label='No Label' and is_personalizable=1
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
;

select 700321+32536;
select 732857/1806645;

# Variation and personalizable combination
SELECT 
  variation_count,
  is_personalizable,
  count(listing_id) as attribute_count
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` USING (listing_id)
WHERE perso_custo_label='No Label'
GROUP BY 1,2
;


-- take sample for qa
WITH active_label as (
SELECT 
pc.*,
row_number() over(partition by perso_custo_label) as rn
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` pc
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` using (listing_id)
WHERE is_active = 1
)

SELECT 
perso_custo_label,
listing_id
FROM active_label
where rn<=20
;
