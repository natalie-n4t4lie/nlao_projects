--Listing Page Complete the Look V1.1 
--https://atlas.etsycorp.com/catapult/1110597656151

--Generate basic dataset using this script: module_engagement_script.sql


--Part 1: Model Output

--Price: Avg. price difference from target ($ and %). Does this vary by target subcategory?
select
"all" as subcategory,
avg((price_usd-target_price_usd)/100) as avg_difference_usd,
avg((price_usd-target_price_usd)/target_price_usd) as avg_difference_pct
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
group by 1
union all 
select
split(ta.full_path, ".")[safe_offset(1)] as subcategory,
avg((price_usd-target_price_usd)/100) as avg_difference_usd,
avg((price_usd-target_price_usd)/target_price_usd) as avg_difference_pct
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
join `etsy-data-warehouse-prod.materialized.listing_taxonomy` ta on r.target_taxonomy_id = ta.taxonomy_id
group by 1
order by 1
;


--Price: Within a set of delivered listings, what is the avg. price variance (are they mostly similar in price or spread out?)


--Taxonomy: Within a set of delivered listings, number of categories,subcategories,taxonomies recommended.  Does this vary by target subcategory?

--Taxonomy: number of listings in different category,subcategory,taxonomy than target listing


--Part 2: Engagement
--Overall Module Click Rate & Purchase Rate
select
ab_variant,
module_placement,
count(distinct visit_id) as visits,
count(distinct browser_id) as browsers,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
where ab_variant = "on"
group by 1,2
order by 1,2;

--Listing Click Through Rate = Clicks / Impressions
--Listing Purchase Rate = Purchases / Clicks
--Browser CTR = Browsers with a Click / Browsers with an Impression
--And so on

--Segment by Target Price
select
case when target_price_usd/100 <= 10 then "0-10"
when target_price_usd/100 <= 50 then "10-50"
when target_price_usd/100 <= 100 then "50-100"
when target_price_usd/100 <= 250 then "100-250"
when target_price_usd/100 <= 500 then "250-500"
when target_price_usd/100 > 500 then "500+"
end as target_price_range,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
group by 1
order by 1;

--Segment by Candidate Price
select
case when price_usd/100 <= 10 then "0-10"
when price_usd/100 <= 50 then "10-50"
when price_usd/100 <= 100 then "50-100"
when price_usd/100 <= 250 then "100-250"
when price_usd/100 <= 500 then "250-500"
when price_usd/100 > 500 then "500+"
end as candidate_price_range,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
group by 1
order by 1;

--Segment by Target Price and Candidate Price Difference (Figure out based on the distribution what "buckets" make sense)
select
case when target_price_usd/100 <= 10 then "0-10"
when target_price_usd/100 <= 50 then "10-50"
when target_price_usd/100 <= 100 then "50-100"
when target_price_usd/100 <= 250 then "100-250"
when target_price_usd/100 <= 500 then "250-500"
when target_price_usd/100 > 500 then "500+"
end as target_price_range,
case when round((price_usd - target_price_usd)/target_price_usd,1) <= 2 then  round((price_usd - target_price_usd)/target_price_usd,1) else 2.1 end as price_diff_pct,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings`
group by 1,2
order by 1,2;


--Segment by Target Listing Subcategory (for which subcategories are complete the look recs most helpful/relevant?)
WITH cte AS (
select
target_taxonomy_id,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
group by 1
)
select
t.second_level_cat_new,
sum(imp) as imp,
sum(clicks) as clicks,
sum(purchases) as purchases
from cte r
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t
  ON r.target_taxonomy_id = t.taxonomy_id
WHERE top_level_cat_new = 'home_and_living'
group by 1
order by 1;


--Segment by Target - Candidate Subcategory pairs (for a given subcategory that buyer is viewing, what type of complete the look rec gets the most/least engagement)
BEGIN
CREATE TEMP TABLE stats AS (
select
target_taxonomy_id,
taxonomy_id,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
group by 1,2
);

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ctl_subcat_pair` AS (
select
t1.second_level_cat_new AS target_sec_lvl_taxo,
t2.second_level_cat_new as candidate_sec_lvl_taxo,
sum(imp) as imp,
sum(clicks) as clicks,
sum(purchases) as purchases
from stats r
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id  
WHERE t1.top_level_cat_new = 'home_and_living' 
  AND t1.second_level_cat_new IS NOT NULL 
  AND t2.second_level_cat_new IS NOT NULL
group by 1,2
);
END;

select
target_sec_lvl_taxo,
t2.second_level_cat_new as candidate_sec_lvl_taxo,
sum(cast(imp as numeric)) as imp2,
sum(clicks) as clicks,
sum(purchases) as purchases
from pair r
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id
WHERE t2.second_level_cat_new IS NOT NULL
group by 1,2
;
END;

BEGIN
CREATE TEMP TABLE stats AS (
select
target_taxonomy_id,
taxonomy_id,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
group by 1,2
);

CREATE TEMP TABLE pair AS (
select
second_level_cat_new AS target_sec_lvl_taxo,
r.taxonomy_id,
sum(imp) as imp,
sum(clicks) as clicks,
sum(purchases) as purchases
from stats r
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
WHERE t1.top_level_cat_new = 'home_and_living' AND second_level_cat_new IS NOT NULL
group by 1,2
);

select
target_sec_lvl_taxo,
t2.second_level_cat_new as candidate_sec_lvl_taxo,
-- sum(imp) as imp,
sum(clicks) as clicks,
sum(purchases) as purchases
from pair r
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id
WHERE t2.second_level_cat_new IS NOT NULL
group by 1,2
;
END;

--Duplicate listings: What percent of module deliveries contain duplicate listing ids?  How does this impact click rate?

WITH cte AS (
SELECT
visit_id,
sequence_number,
target_listing_id,
COUNT(DISTINCT listing_id) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_listing
-- CASE WHEN COUNT(DISTINCT listing_id) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) < 6 THEN 1 ELSE 0 END AS saw_duplicate
FROM `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
)
SELECT
distinct_listing,
COUNT(visit_id) AS visit_count,
COUNT(distinct visit_id) AS visit_distinct_count
FROM cte
GROUP BY 1
;-- No duplicate listings


--Browser segmentation: Segment by Perso Journey Segment
--For a given browser, join their first bucketed visit to rollups.perso_segment_visits to assign segments
--Then segment by the variables "we_know", "visit_recency", and "browse_buy" and calculate browser level engagement metrics.s




