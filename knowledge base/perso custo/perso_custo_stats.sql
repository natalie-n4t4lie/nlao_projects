# ACTIVE LISTING COVERAGE AND GMS
SELECT 
COUNT(distinct CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN listing_id ELSE 0 END) as perso_custo_listing_cout,
COUNT(distinct CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN listing_id ELSE 0 END) / count(distinct listing_id) as listing_coverage,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN past_year_gms ELSE 0 END) as perso_custo_gms,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN past_year_gms ELSE 0 END) / sum(alb.past_year_gms) as gms_coverage
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
;

# category coverage
SELECT 
alb.top_category,
COUNT(distinct CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN listing_id ELSE 0 END) as perso_custo_listing_cout,
COUNT(distinct CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN listing_id ELSE 0 END) / count(distinct listing_id) as listing_coverage,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN past_year_gms ELSE 0 END) as perso_custo_gms,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` WHERE perso_custo_label!='No Label') THEN past_year_gms ELSE 0 END) / sum(alb.past_year_gms) as gms_coverage
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
GROUP BY 1
;

#active listing with perso/custo keywords
SELECT
count(distinct listing_id),
sum(past_year_gms)
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE (LOWER(title) LIKE '%personali%' AND LOWER(title) NOT LIKE '%personality%')
   OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
   OR REGEXP_CONTAINS(LOWER(title),r'monogram|made to order|made-to-order')
;
-- 7274968
-- 1934846663.17


#giftiness avg giftiness
with perso_custo_listing as 
(SELECT
distinct listing_id
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
WHERE perso_custo_label!='No Label')

SELECT 
avg(g.overall_giftiness) as avg_giftiness
FROM perso_custo_listing
JOIN `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` g USING (LISTING_ID)
WHERE _date=current_date()
;

#gifitness score distribution
with perso_custo_listing as 
(SELECT
distinct listing_id
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level`
WHERE perso_custo_label!='No Label')

SELECT 
round(g.overall_giftiness,1) as rounded_giftiness,
count(listing_id) as frequency
FROM perso_custo_listing
JOIN `etsy-data-warehouse-prod.knowledge_base.listing_giftiness` g USING (LISTING_ID)
WHERE _date=current_date()
group by 1
order by 1 asc
;

#waterfall chart for steps of removing listings
SELECT
  count(listing_id)
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE (LOWER(title) LIKE '%personali%' AND LOWER(title) NOT LIKE '%personality%')
   OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
   OR REGEXP_CONTAINS(LOWER(title),r'monogram|made to order|made-to-order')
;--7263179

--first filter with variation and personalization field
with listings as (
SELECT
  listing_id,
  title
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
WHERE (LOWER(title) LIKE '%personali%' AND LOWER(title) NOT LIKE '%personality%')
   OR (LOWER(title) LIKE '%custom%' AND LOWER(title) NOT LIKE '%customer%')
   OR REGEXP_CONTAINS(LOWER(title),r'monogram|made to order|made-to-order')
)
SELECT 
 count(distinct listing_id)
FROM listings ll 
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` l USING (listing_id)
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` la USING (listing_id)
WHERE 
  is_personalizable = 1 OR variation_count in (1,2)
;--6035981

SELECT count(distinct listing_id)
FROM `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` 
WHERE perso_custo_label='No Label'
;--1273156


SELECT 
sum(past_year_gms) as gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
;--8690280966.66

#concept level coverage
SELECT 
full_label,
SUM(past_year_gms) as gms,
SUM(past_year_gms) / 8690280966.66 as  pct_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` USING (listing_id)
GROUP BY 1
;

#perso custo label level coverage

SELECT 
perso_custo_label,
SUM(past_year_gms) as gms,
SUM(past_year_gms) / 8690280966.66 as  pct_gms
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
JOIN `etsy-data-warehouse-prod.knowledge_base.perso_custo_label_level` USING (listing_id)
GROUP BY 1
;




