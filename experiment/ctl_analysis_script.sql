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
select
split(t1.full_path, '.')[offset(0)] as target_top_cat,
case when array_length(split(t1.full_path,'.'))>1 then split(t1.full_path, '.')[offset(1)] else null end as target_second_cat,
case when array_length(split(t1.full_path,'.'))>2 then split(t1.full_path, '.')[offset(2)] else null end as target_third_cat,
count(*) as imp,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
group by 1,2,3
order by 1,2,3;


--Segment by Target - Candidate Subcategory pairs (for a given subcategory that buyer is viewing, what type of complete the look rec gets the most/least engagement)
SELECT
  split(t1.full_path, '.')[offset(0)] as target_top_cat,
  case when array_length(split(t1.full_path,'.'))>1 then split(t1.full_path, '.')[offset(1)] else null end as target_second_cat,
  case when array_length(split(t1.full_path,'.'))>2 then split(t1.full_path, '.')[offset(2)] else null end as target_third_cat,
  split(t2.full_path, '.')[offset(0)] as candidate_top_cat,
  case when array_length(split(t2.full_path,'.'))>1 then split(t2.full_path, '.')[offset(1)] else null end as candidate_second_cat,
  case when array_length(split(t2.full_path,'.'))>2 then split(t2.full_path, '.')[offset(2)] else null end as candidate_third_cat,
  count(*) as imp,
  sum(clicked_flag) as clicks,
  sum(purchased_flag) as purchases
FROM `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6;


--Duplicate listings: What percent of module deliveries contain duplicate listing ids?  How does this impact click rate?

WITH cte AS (
SELECT
visit_id,
sequence_number,
target_listing_id,
COUNT(DISTINCT listing_id) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_listing_count
FROM `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
)
SELECT
distinct_listing_count,
COUNT(*) AS imp,
FROM cte
GROUP BY 1
;-- No duplicate listings


-- target and candidate listing similarity
WITH cte AS (
select
visit_id,
clicked_flag,
purchased_flag,
sequence_number,
target_listing_id,
target_taxonomy_id,
split(t1.full_path, '.')[offset(0)] as target_top_cat,
case when array_length(split(t1.full_path,'.'))>1 then split(t1.full_path, '.')[offset(1)] else null end as target_second_cat,
case when array_length(split(t1.full_path,'.'))>2 then split(t1.full_path, '.')[offset(2)] else null end as target_third_cat,
split(t2.full_path, '.')[offset(0)] as candidate_top_cat,
listing_id,
r.taxonomy_id,
case when array_length(split(t2.full_path,'.'))>1 then split(t2.full_path, '.')[offset(1)] else null end as candidate_second_cat,
case when array_length(split(t2.full_path,'.'))>2 then split(t2.full_path, '.')[offset(2)] else null end as candidate_third_cat,
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id
)
SELECT
target_top_cat,
CASE WHEN target_top_cat = candidate_top_cat THEN 1 ELSE 0 END AS same_top_cat,
CASE WHEN target_second_cat IS NOT NULL THEN 1 ELSE 0 END AS has_sec_cat,
CASE WHEN target_top_cat = candidate_top_cat AND  target_second_cat = candidate_second_cat THEN 1 ELSE 0 END AS same_sec_cat,
CASE WHEN target_third_cat IS NOT NULL THEN 1 ELSE 0 END AS has_third_cat,
CASE WHEN target_top_cat = candidate_top_cat AND  target_second_cat = candidate_second_cat AND target_third_cat = candidate_third_cat THEN 1 ELSE 0 END AS same_third_cat,
CASE WHEN target_taxonomy_id = taxonomy_id THEN 1 ELSE 0 END AS same_taxo,
COUNT(visit_id) AS visit_count,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
FROM cte
GROUP BY 1,2,3,4,5,6,7
;

select
t1.full_path,
count(*) AS count
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
WHERE r.taxonomy_id = r.target_taxonomy_id
GROUP BY 1
ORDER BY 2 DESC;

select
t1.full_path,
ln.flag,
count(*) AS count
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
JOIN `etsy-data-warehouse-dev.nlao.deepest_leaf_nodes` ln
  ON r.taxonomy_id = ln.taxonomy_id
WHERE r.taxonomy_id = r.target_taxonomy_id
GROUP BY 1,2
ORDER BY 3 DESC;

-- candidate and candidate similarity

-- distinct taxo_id/first level/second level/thid level
WITH cte AS (
select
visit_id,
clicked_flag,
purchased_flag,
sequence_number,
target_listing_id,
r.taxonomy_id,
split(t2.full_path, '.')[offset(0)] as candidate_top_cat,
case when array_length(split(t2.full_path,'.'))>1 then split(t2.full_path, '.')[offset(1)] else null end as candidate_second_cat,
case when array_length(split(t2.full_path,'.'))>2 then split(t2.full_path, '.')[offset(2)] else null end as candidate_third_cat
from `etsy-data-warehouse-dev.nlao.recs_exp_listings` r
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
  ON r.target_taxonomy_id = t1.taxonomy_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t2
  ON r.taxonomy_id = t2.taxonomy_id
),
cat_count AS (
SELECT
visit_id,
clicked_flag,
purchased_flag,
sequence_number,
target_listing_id,
COUNT(DISTINCT taxonomy_id) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_candidate_taxo,
COUNT(DISTINCT candidate_top_cat) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_candidate_l1_cat,
COUNT(DISTINCT candidate_second_cat) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_candidate_l2_cat,
COUNT(DISTINCT candidate_third_cat) OVER (PARTITION BY visit_id, sequence_number, target_listing_id) AS distinct_candidate_l3_cat,
FROM cte
)  
SELECT
distinct_candidate_taxo,
distinct_candidate_l1_cat,
distinct_candidate_l2_cat,
distinct_candidate_l3_cat,
COUNT(visit_id) AS visit_count,
sum(clicked_flag) as clicks,
sum(purchased_flag) as purchases
FROM cat_count
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;

--Browser segmentation: Segment by Perso Journey Segment
--For a given browser, join their first bucketed visit to rollups.perso_segment_visits to assign segments
--Then segment by the variables "we_know", "visit_recency", and "browse_buy" and calculate browser level engagement metrics.s




