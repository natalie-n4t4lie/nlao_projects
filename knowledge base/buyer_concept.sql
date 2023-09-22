-- CREATE A UNION TABLE THAT GATHERS DATA FROM ALL BUYER CONCEPT MODELS
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.buyer_concept_union` AS (
SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies`
WHERE _date = '2022-05-23' AND score >= 0.1 
UNION ALL
SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals`
WHERE _date = '2022-05-23' AND score >= 0.1 
UNION ALL
SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles`
WHERE _date = '2022-05-23' AND score >= 0.1 
);
################### COVERAGE ###################
-- OVERALL (ACTIVE BUYER AS DENOMINATOR)
SELECT
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS animal_coverage,
COUNT(CASE WHEN bb.is_active = 1 THEN bb.mapped_user_id ELSE NULL END) as active_user_count,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_hobby_coverage,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` bb
;

-- OVERALL (USER VISIT IN THE PAST 30 DAYS AS DENOMINATOR)
SELECT
COUNT(user_id) AS user_visit,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23" AND user_id is not null
;

-- REGION
SELECT
bb.region_name,
COUNT(bb.mapped_user_id) as user_count,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

-- PAST YEAR GMS (CUSTOM BIN)
WITH buyer_gms_bin AS (
SELECT 
mapped_user_id,
CASE
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 0 AND 25.0 THEN '$0-$25'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 25.01 AND 50.0 THEN '$25-$50'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 50.01 AND 75.0 THEN '$50-$75'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 75.01 AND 100.0 THEN '$75-$100'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 100.01 AND 250.0 THEN '$100-$250'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 250.01 AND 500.0 THEN '$250-$500'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 500.01 AND 750.0 THEN '$500-$750'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 750.01 AND 1000.0 THEN '$750-$1000'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) > 1000 THEN '$1000+'
        ELSE cast(round(cast(round(p.past_year_gms,20) as numeric),2) as string)
        END AS buyer_past_year_gms_bin,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` p
)
SELECT
bb.buyer_past_year_gms_bin,
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS animal_coverage,
FROM buyer_gms_bin bb
GROUP BY 1
;

-- LIFETIME GMS BIN
SELECT
bb.buyer_lifetime_gms_bin,
COUNT(bb.mapped_user_id) as user_count,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

-- BUYER SEGMENT
SELECT
bb.buyer_segment,
COUNT(bb.mapped_user_id) as user_count,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

-- VISIT SEGMENT (AVG)
WITH model_audience AS (
SELECT 
user_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
visit_level AS (
SELECT
v.user_id,
count(distinct _date) AS visit_day_count
FROM model_audience
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v USING (user_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT

avg(visit_day_count) as avg_visit_day
FROM visit_level bb
GROUP BY 1
;

-- VISIT SEGMENT 
WITH model_audience AS (
SELECT 
user_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
visit_level AS (
SELECT
v.user_id,
count(distinct _date) AS visit_day_count
FROM model_audience
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v USING (user_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23" 
GROUP BY 1
)
SELECT
CASE
        WHEN visit_day_count BETWEEN  1 AND 10 THEN CAST(visit_day_count AS STRING)
        WHEN visit_day_count BETWEEN 11 AND 20 THEN '11-20'
        WHEN visit_day_count BETWEEN 21 AND 30 THEN '21-30'
        WHEN visit_day_count BETWEEN 31 AND 40 THEN '31-40'
        WHEN visit_day_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN visit_day_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN visit_day_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS visit_day_count,
count(distinct user_id) as user_count,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM visit_level bb
GROUP BY 1
;

-- LISTING VIEW (AVG)
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
listing_views AS (
SELECT
user_id,
count(distinct listing_id) AS listing_view_distinct,
count(listing_id) AS listing_view
FROM model_audience
JOIN `etsy-data-warehouse-prod.analytics.listing_views`  USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN listing_view_distinct ELSE NULL END) AS hobby_distinct_listing_view,
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN listing_view_distinct ELSE NULL END) AS animal_distinct_listing_view,
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN listing_view_distinct ELSE NULL END) AS style_distinct_listing_view,
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN listing_view ELSE NULL END) AS hobby_listing_view,
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN listing_view ELSE NULL END) AS animal_listing_view,
AVG(CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN listing_view ELSE NULL END) AS style_listing_view
FROM listing_views bb
GROUP BY 1
;

-- LISTING VIEW BIN
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
listing_views AS (
SELECT
user_id,
count(distinct listing_id) AS listing_view_distinct,
count(listing_id) AS listing_view
FROM model_audience
JOIN `etsy-data-warehouse-prod.analytics.listing_views`  USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
CASE
        WHEN listing_view BETWEEN  1 AND 20 THEN '01-20'
        WHEN listing_view BETWEEN 21 AND 30 THEN '21-40'
        WHEN listing_view BETWEEN 41 AND 60 THEN '41-60'
        WHEN listing_view BETWEEN 61 AND 80 THEN '61-80'
        WHEN listing_view BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS listing_view_bin,
CASE
        WHEN listing_view_distinct BETWEEN  1 AND 10 THEN '01-10'
        WHEN listing_view_distinct BETWEEN 11 AND 20 THEN '11-20'
        WHEN listing_view_distinct BETWEEN 21 AND 30 THEN '21-30'
        WHEN listing_view_distinct BETWEEN 31 AND 40 THEN '31-40'
        WHEN listing_view_distinct BETWEEN 41 AND 50 THEN '41-50'
        ELSE '50+'
      END AS listing_view_distinct_bin,      
count(distinct user_id) as user_count,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM listing_views bb
GROUP BY 1,2
;

-- LISTING VIEW
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
listing_views AS (
SELECT
user_id,
count(distinct listing_id) AS listing_view_distinct,
count(listing_id) AS listing_view
FROM model_audience
JOIN `etsy-data-warehouse-prod.analytics.listing_views`  USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
CASE
        WHEN listing_view BETWEEN  1 AND 40 THEN CAST(listing_view AS STRING)
        WHEN listing_view BETWEEN 41 AND 60 THEN '41-60'
        WHEN listing_view BETWEEN 61 AND 80 THEN '61-80'
        WHEN listing_view BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS listing_view_bin,
CASE
        WHEN listing_view_distinct BETWEEN  1 AND 30 THEN CAST(listing_view_distinct AS STRING)
        WHEN listing_view_distinct BETWEEN 31 AND 40 THEN '31-40'
        WHEN listing_view_distinct BETWEEN 41 AND 50 THEN '41-50'
        ELSE '50+'
      END AS listing_view_distinct_bin,      
count(distinct user_id) as user_count,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM listing_views bb
GROUP BY 1,2
;

-- INTERACTION: SEARCH QUERY
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
search_queries AS (
SELECT
m.user_id,
count(distinct query) AS query_count,
FROM model_audience m
JOIN `etsy-data-warehouse-prod.search.events` s ON  m.user_id = cast(s.user_id as int64)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
CASE
        WHEN query_count BETWEEN  1 AND 40 THEN CAST(query_count AS STRING)
        WHEN query_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN query_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN query_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS query_count_bin,  
count(distinct user_id) as user_count,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM search_queries bb
GROUP BY 1
;

-- INTERACTION: PURCHASES
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
purchase_past_3_months AS (
SELECT
m.user_id,
count(distinct transaction_id) AS transaction_count,
FROM model_audience m
JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` s USING (user_id)
WHERE date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
CASE
        WHEN transaction_count BETWEEN  1 AND 40 THEN CAST(transaction_count AS STRING)
        WHEN transaction_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN transaction_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN transaction_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS transaction_count,  
count(distinct user_id) as user_count,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Hobby') THEN user_id ELSE NULL END) AS hobby,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Animal') THEN user_id ELSE NULL END) AS animal,
COUNT(distinct CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union` WHERE attribute_type = 'Style') THEN user_id ELSE NULL END) AS style
FROM purchase_past_3_months bb
GROUP BY 1
;

################### CONCEPT PER BUYER  ###################
WITH cte AS(
SELECT
user_id,
attribute_type,
COUNT(DISTINCT display_name) AS concept_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1,2
)
SELECT 
attribute_type,
concept_count,
COUNT(user_id) as user_count
FROM cte
GROUP BY 1,2
;

-- AVERAGE COUNT
WITH cte AS(
SELECT
user_id,
attribute_type,
COUNT(DISTINCT display_name) AS concept_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1,2
)
SELECT 
attribute_type,
avg(concept_count) as avg_concept_count,
FROM cte
GROUP BY 1
;

-- VISIT DAY 
WITH model_audience AS (
SELECT 
user_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
visit_level AS (
SELECT
v.user_id,
count(distinct _date) AS visit_day_count
FROM model_audience
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v USING (user_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23" 
GROUP BY 1
),
concept_per_buyer AS(
SELECT
user_id,
attribute_type,
count(*) as concept_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1,2
)
SELECT
CASE
        WHEN visit_day_count BETWEEN  1 AND 10 THEN CAST(visit_day_count AS STRING)
        WHEN visit_day_count BETWEEN 11 AND 20 THEN '11-20'
        WHEN visit_day_count BETWEEN 21 AND 30 THEN '21-30'
        WHEN visit_day_count BETWEEN 31 AND 40 THEN '31-40'
        WHEN visit_day_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN visit_day_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN visit_day_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS visit_day_count,
attribute_type,
concept_count,
count(distinct user_id) as user_count
FROM visit_level bb
JOIN concept_per_buyer USING (user_id)
GROUP BY 1,2,3
;

################### INTEREST POPULARITY ###################
WITH listing_level AS (
SELECT
attribute_type,
display_name,
COUNT(DISTINCT listing_id) as listing_count,
SUM(past_year_gms) AS listing_gms
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests`
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING(listing_id)
WHERE _date = date_sub(current_date(), interval 1 day) 
      AND score >=0.05
      AND (attribute_type = 'Hobby' OR attribute_type = 'Animal' OR attribute_type = 'Style')
GROUP BY 1,2
)
,user_level AS (
SELECT
attribute_type,
display_name,
COUNT(DISTINCT user_id) as user_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1,2
)
SELECT
l.attribute_type,
l.display_name,
l.listing_count,
u.user_count,
l.listing_gms,
rank() OVER (PARTITION BY l.attribute_type ORDER BY listing_count DESC) as listing_rank,
rank() OVER (PARTITION BY l.attribute_type ORDER BY user_count DESC) as user_rank,
rank() OVER (PARTITION BY l.attribute_type ORDER BY listing_gms DESC) as listing_gms_rank,
FROM listing_level l
LEFT JOIN USER_LEVEL u 
ON l.attribute_type = u.attribute_type AND l.display_name = u.display_name
;

-- STYLE GMS Coverage
WITH overall as (
SELECT
count(listing_id) as total_active_listing_count,
sum(past_year_gms) as total_active_listing_gms,
1 as dummy
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
),
listing_level AS (
SELECT
attribute_type,
display_name,
COUNT(DISTINCT listing_id) as listing_count,
SUM(past_year_gms) AS listing_gms,
1 as dummy
FROM `etsy-data-warehouse-prod.knowledge_base.listing_interests`
JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING(listing_id)
WHERE _date = date_sub(current_date(), interval 1 day) 
      AND score >=0.05
      AND attribute_type = 'Style'
GROUP BY 1,2
)

SELECT
attribute_type,
display_name,
listing_count / total_active_listing_count as listing_gms_coverage,
listing_gms / total_active_listing_gms as listing_gms_coverage
FROM listing_level
JOIN overall USING (dummy)
;


################### OVERALL CONFIDENCE SCORE STATS #########################

-- MEAN
SELECT
attribute_type,
ROUND(avg(score),2) as avg_score
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1
;

-- MEDIAN
SELECT
   DISTINCT attribute_type, 
   PERCENTILE_DISC(ROUND(score,2), 0.5) OVER (PARTITION BY attribute_type) AS median_score
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
ORDER BY attribute_type
;

-- MODE
WITH cte AS (
SELECT
attribute_type,
ROUND(score,2) as score,
RANK() OVER (PARTITION BY attribute_type ORDER BY COUNT(DISTINCT user_id) DESC) as rk
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1,2
)
SELECT
attribute_type,
score
FROM cte
WHERE rk=1
;

################### CONFIDENCE SCORE DISTRIBUTION #########################

-- OVERALL
with union_table AS (SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies`
WHERE _date = '2022-05-23' 
UNION ALL
SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals`
WHERE _date = '2022-05-23'
UNION ALL
SELECT 
user_id,
display_name,
score,
attribute_type
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles`
WHERE _date = '2022-05-23'
)
SELECT
attribute_type,
ROUND(score,1) AS score_round,
COUNT(*) as count
FROM union_table
GROUP BY 1,2
ORDER BY 1,2 ASC
;

-- BUYER GMS BIN (AVG SCORE)
WITH buyer_gms_bin AS (
SELECT 
mapped_user_id as user_id,
CASE
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 0 AND 25.0 THEN '$0-$25'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 25.01 AND 50.0 THEN '$25-$50'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 50.01 AND 75.0 THEN '$50-$75'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 75.01 AND 100.0 THEN '$75-$100'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 100.01 AND 250.0 THEN '$100-$250'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 250.01 AND 500.0 THEN '$250-$500'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 500.01 AND 750.0 THEN '$500-$750'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 750.01 AND 1000.0 THEN '$750-$1000'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) > 1000 THEN '$1000+'
        ELSE '$0'
        END AS buyer_past_year_gms_bin,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` p
)
SELECT
attribute_type,
buyer_past_year_gms_bin,
avg(score) as avg_score
FROM buyer_gms_bin bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2
;

-- BUYER GMS BIN (SCORE DISTRIBUTION)
WITH buyer_gms_bin AS (
SELECT 
mapped_user_id as user_id,
CASE
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 0 AND 25.0 THEN '$0-$25'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 25.01 AND 50.0 THEN '$25-$50'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 50.01 AND 75.0 THEN '$50-$75'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 75.01 AND 100.0 THEN '$75-$100'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 100.01 AND 250.0 THEN '$100-$250'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 250.01 AND 500.0 THEN '$250-$500'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 500.01 AND 750.0 THEN '$500-$750'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) BETWEEN 750.01 AND 1000.0 THEN '$750-$1000'
        WHEN round(cast(round(p.past_year_gms,20) as numeric),2) > 1000 THEN '$1000+'
        ELSE '$0'
        END AS buyer_past_year_gms_bin,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` p
)
SELECT
attribute_type,
buyer_past_year_gms_bin,
ROUND(score,1) AS score_bin,
COUNT(DISTINCT user_id) AS user_count
FROM buyer_gms_bin bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;

-- VISIT SEGMENT (AVG SCORE)
WITH model_audience AS (
SELECT 
user_id,
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
),
visit_count AS (
SELECT
user_id,
count(distinct (date(start_datetime))) AS visit_day_count
FROM model_audience
JOIN `etsy-data-warehouse-prod.weblog.recent_visits`  USING(user_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
CASE
        WHEN visit_day_count BETWEEN  1 AND 10 THEN '01-10D'
        WHEN visit_day_count BETWEEN 11 AND 20 THEN '11-20D'
        WHEN visit_day_count BETWEEN 21 AND 30 THEN '21-30D'
        WHEN visit_day_count BETWEEN 31 AND 40 THEN '31-40D'
        WHEN visit_day_count BETWEEN 41 AND 50 THEN '41-50D'
  END AS visit_day_bin,
avg(score) as avg_score
FROM visit_count bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2
;

-- VISIT SEGMENT (SCORE DISTRIBUTION)
WITH model_audience AS (
SELECT 
user_id,
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
),
visit_count AS (
SELECT
user_id,
count(distinct (date(start_datetime))) AS visit_day_count
FROM model_audience
JOIN `etsy-data-warehouse-prod.weblog.recent_visits`  USING(user_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
ROUND(score,1) AS score_bin,
CASE
        WHEN visit_day_count BETWEEN  1 AND 10 THEN '01-10D'
        WHEN visit_day_count BETWEEN 11 AND 20 THEN '11-20D'
        WHEN visit_day_count BETWEEN 21 AND 30 THEN '21-30D'
        WHEN visit_day_count BETWEEN 31 AND 40 THEN '31-40D'
        WHEN visit_day_count BETWEEN 41 AND 50 THEN '41-50D'
  END AS visit_day_bin,
COUNT(DISTINCT user_id) AS user_count
FROM visit_count bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;

-- LISTING VIEW BIN
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
listing_views AS (
SELECT
user_id,
count(distinct listing_id) AS listing_view_distinct,
count(listing_id) AS listing_view
FROM model_audience
JOIN `etsy-data-warehouse-prod.analytics.listing_views`  USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
round(score,1) as score,
CASE
        WHEN listing_view_distinct BETWEEN  1 AND 10 THEN '01-10'
        WHEN listing_view_distinct BETWEEN 11 AND 20 THEN '11-20'
        WHEN listing_view_distinct BETWEEN 21 AND 30 THEN '21-30'
        WHEN listing_view_distinct BETWEEN 31 AND 40 THEN '31-40'
        WHEN listing_view_distinct BETWEEN 41 AND 50 THEN '41-50'
        ELSE '50+'
      END AS listing_view_distinct_bin, 
count(distinct user_id) as user_count,
FROM listing_views 
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;

-- INTERACTION: LISTING VIEW BIN
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
listing_views AS (
SELECT
user_id,
count(distinct listing_id) AS listing_view_distinct,
count(listing_id) AS listing_view
FROM model_audience
JOIN `etsy-data-warehouse-prod.analytics.listing_views`  USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
round(score,1) as score,
CASE
        WHEN listing_view_distinct BETWEEN  1 AND 10 THEN '01-10'
        WHEN listing_view_distinct BETWEEN 11 AND 20 THEN '11-20'
        WHEN listing_view_distinct BETWEEN 21 AND 30 THEN '21-30'
        WHEN listing_view_distinct BETWEEN 31 AND 40 THEN '31-40'
        WHEN listing_view_distinct BETWEEN 41 AND 50 THEN '41-50'
        ELSE '50+'
      END AS listing_view_distinct_bin, 
count(distinct user_id) as user_count
FROM listing_views 
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;

-- INTERACTION: PURCHASES
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
purchase_past_3_months AS (
SELECT
m.user_id,
count(distinct transaction_id) AS transaction_count,
FROM model_audience m
JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_visits` s USING (user_id)
WHERE date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
round(score,1) as score,
CASE
        WHEN transaction_count BETWEEN  1 AND 40 THEN CAST(transaction_count AS STRING)
        WHEN transaction_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN transaction_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN transaction_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS transaction_count,  
count(distinct user_id) as user_count
FROM purchase_past_3_months bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;

-- INTERACTION: SEARCH QUERY
WITH model_audience AS (
SELECT 
user_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.recent_visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
      AND user_id IS NOT NULL
),
search_queries AS (
SELECT
m.user_id,
count(distinct query) AS query_count,
FROM model_audience m
JOIN `etsy-data-warehouse-prod.search.events` s ON  m.user_id = cast(s.user_id as int64)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"
GROUP BY 1
)
SELECT
attribute_type,
round(score,1) as score,
CASE
        WHEN query_count BETWEEN  1 AND 40 THEN CAST(query_count AS STRING)
        WHEN query_count BETWEEN 41 AND 60 THEN '41-60'
        WHEN query_count BETWEEN 61 AND 80 THEN '61-80'
        WHEN query_count BETWEEN 81 AND 100 THEN '81-100'
        ELSE '100+'
      END AS query_count_bin,  
count(distinct user_id) as user_count
FROM search_queries bb
JOIN `etsy-data-warehouse-dev.nlao.buyer_concept_union` USING (user_id)
GROUP BY 1,2,3
;
