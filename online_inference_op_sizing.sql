
--=================--
--| Current State |--
--=================--
-- What's visit traffic share of sign-in vs non sign-in traffic?
SELECT 
CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END AS user_id_flag,
COUNT(DISTINCT user_id) AS user_count,
COUNT(DISTINCT browser_id) AS browser_count,
COUNT(DISTINCT visit_id) AS visit_count
FROM `etsy-data-warehouse-prod.weblog.visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23"
GROUP BY 1
;

-- Becuase buyer concept model ingest user interaction data (listing view, search, purchases) to produce inference. We are going to only count visitors with similar activties (with search/listing views) as those who are likely to have a buyer concept model inference, instead of assuming the model could have a prediction for every visitor.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.buyer_concept_user_level` AS (
WITH eligible_visit AS (
SELECT 
user_id,
browser_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23" AND user_id IS NOT NULL
),  
listing_view AS (
SELECT
user_id,
COUNT(DISTINCT listing_id) as listing_view_count
FROM eligible_visit
JOIN `etsy-data-warehouse-prod.analytics.listing_views` USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23" -- LOOK BACK 3 MONTHS FOR ACTIVITIES
GROUP BY 1
),
search_views AS(
SELECT
e.user_id,
COUNT(DISTINCT l.query) AS search_query_count
FROM eligible_visit e 
JOIN `etsy-data-warehouse-prod.search.events` l USING (visit_id)
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"-- LOOK BACK 3 MONTHS FOR ACTIVITIES
GROUP BY 1 
)
SELECT
e.user_id,
coalesce(listing_view_count,0) AS listing_view_count,
coalesce(search_query_count,0) AS search_query_count
FROM eligible_visit e
LEFT JOIN listing_view l 
  ON e.user_id = l.user_id
LEFT JOIN search_views s 
  ON e.user_id = s.user_id
)
;

SELECT 
count(distinct browser_id),
count(distinct visit_id)
FROM `etsy-data-warehouse-prod.weblog.visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23" AND user_id in (select user_id from `etsy-data-warehouse-dev.nlao.buyer_concept_union`)
;

-- Find out search and view combo and coverage of each combo
SELECT
CASE WHEN user_id IN (SELECT user_id FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`) THEN 1 ELSE 0 END AS buyer_concept_flag,
CASE WHEN search_query_count <=3 THEN cast(search_query_count as string) ELSE '4+' END as search_query_count,
CASE WHEN listing_view_count <=3 THEN cast(listing_view_count as string) ELSE '4+' END as listing_view_count,
COUNT(DISTINCT user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_user_level`
GROUP BY 1,2,3
;

WITH concept AS (
SELECT
user_id,
count(*) as concept_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_union`
GROUP BY 1
)
SELECT
CASE WHEN listing_view_count <= 6 THEN cast(listing_view_count as string) ELSE '7+' END as listing_view_count,
coalesce(CASE WHEN concept_count <= 6 THEN cast(concept_count as string) ELSE '7+' END,'0') as concept_count,
COUNT(DISTINCT user_id) AS user_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_user_level`
LEFT JOIN concept USING (user_id)
GROUP BY 1,2
;

-- VISIT LEVEL
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.buyer_concept_visit_level` AS (
WITH eligible_visit AS (
SELECT
_date, 
visit_id
FROM `etsy-data-warehouse-prod.weblog.visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23" AND user_id IS NULL
),  
listing_view AS (
SELECT
v.visit_id,
COUNT(DISTINCT l.listing_id) as listing_view_count
FROM eligible_visit v
JOIN `etsy-data-warehouse-prod.analytics.listing_views` l 
  ON v.visit_id = l.visit_id -- VISIT_ID CAN'T LOOK BACK 3 MONTHS FOR ACTIVITIES
WHERE v._date = l._date
GROUP BY 1
),
search_views AS(
SELECT
v.visit_id,
COUNT(DISTINCT e.query) as search_query_count
FROM  eligible_visit v
JOIN `etsy-data-warehouse-prod.search.events` e 
  ON v.visit_id = e.visit_id-- VISIT_ID CAN'T LOOK BACK 3 MONTHS FOR ACTIVITIES
WHERE query IS NOT NULL AND v._date = e._date
GROUP BY 1
)
SELECT
e.visit_id AS visit_id,
coalesce(listing_view_count,0) AS listing_view_count,
coalesce(search_query_count,0) AS search_query_count
FROM eligible_visit e
LEFT JOIN listing_view l ON l.visit_id = e.visit_id
LEFT JOIN search_views s ON s.visit_id = e.visit_id
)
;
-- FIND OUT CONDITION STATS
SELECT
CASE WHEN search_query_count <=3 THEN cast(search_query_count as string) ELSE '4+' END as search_query_count,
CASE WHEN listing_view_count <=3 THEN cast(listing_view_count as string) ELSE '4+' END as listing_view_count,
COUNT(DISTINCT visit_id) AS visit_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_visit_level`
GROUP BY 1,2
;


-- BROWSER LEVEL
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.buyer_concept_browser_level` AS (
WITH eligible_visit AS (
SELECT 
browser_id,
visit_id
FROM `etsy-data-warehouse-prod.weblog.visits` 
WHERE _date BETWEEN "2022-04-23" AND "2022-05-23" AND user_id IS NULL
),  
listing_view AS (
SELECT
v.browser_id,
COUNT(DISTINCT l.listing_id) as listing_view_count
FROM eligible_visit v
JOIN `etsy-data-warehouse-prod.analytics.listing_views` l 
  ON v.visit_id = l.visit_id
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23" -- LOOK BACK 3 MONTHS FOR ACTIVITIES
GROUP BY 1
),
search_views AS(
SELECT
v.browser_id,
COUNT(DISTINCT e.query) as search_query_count
FROM  eligible_visit v
JOIN `etsy-data-warehouse-prod.search.events` e 
  ON v.visit_id = e.visit_id 
WHERE _date BETWEEN "2022-02-23" AND "2022-05-23"-- LOOK BACK 3 MONTHS FOR ACTIVITIES
GROUP BY 1 
)
SELECT
e.browser_id,
coalesce(listing_view_count,0) AS listing_view_count,
coalesce(search_query_count,0) AS search_query_count
FROM eligible_visit e
LEFT JOIN listing_view l ON l.browser_id = e.browser_id
LEFT JOIN search_views s ON s.browser_id = e.browser_id
)
;

SELECT
CASE WHEN search_query_count <=3 THEN cast(search_query_count as string) ELSE '4+' END as search_query_count,
CASE WHEN listing_view_count <=3 THEN cast(listing_view_count as string) ELSE '4+' END as listing_view_count,
COUNT(DISTINCT browser_id) AS browser_count
FROM `etsy-data-warehouse-dev.nlao.buyer_concept_browser_level`
GROUP BY 1,2
;

--==================================================--
--| VIEW DIFFERENT CATEGORY IN TWO CONSECUTIVE DAY |--
--==================================================--

WITH visit_data AS (
SELECT
distinct v._date,
v.user_id,
l.taxonomy_id,
l.top_level_cat_new,
l.second_level_cat_new,
l.third_level_cat_new --CATEGORY VIEWED
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.user_mart.user_profile` u
ON v.user_id = u.user_id
JOIN `etsy-data-warehouse-prod.analytics.listing_views` lv
ON v.visit_id = lv.visit_id AND v._date = lv._date
JOIN `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy` l
ON l.listing_id = lv.listing_id
WHERE v._date BETWEEN '2022-07-01' AND '2022-08-01' AND u.is_admin = 0 AND u.is_seller = 0
),
candidate AS (
SELECT
c1.user_id,
max(case when c1.top_level_cat_new = c2.top_level_cat_new then 1 else 0 end) AS view_same_top_level_cat_new,
max(case when c1.top_level_cat_new != c2.top_level_cat_new then 1 else 0 end) AS view_new_top_level_cat_new,
max(case when c1.second_level_cat_new = c2.second_level_cat_new then 1 else 0 end) AS view_same_second_level_cat_new,
max(case when c1.second_level_cat_new != c2.second_level_cat_new then 1 else 0 end) AS view_new_second_level_cat_new,
max(case when c1.third_level_cat_new = c2.third_level_cat_new then 1 else 0 end) AS view_same_third_level_cat_new,
max(case when c1.third_level_cat_new != c2.third_level_cat_new then 1 else 0 end) AS view_new_third_level_cat_new,
max(case when c1.taxonomy_id = c2.taxonomy_id then 1 else 0 end) AS view_same_taxonomy_id,
max(case when c1.taxonomy_id != c2.taxonomy_id then 1 else 0 end) AS view_new_taxonomy_id
FROM visit_data c1
JOIN visit_data c2
ON c1.user_id = c2.user_id and c1._date = date_sub(c2._date, interval 1 day)
GROUP BY 1
)
SELECT
view_same_top_level_cat_new,
view_new_top_level_cat_new,
view_same_second_level_cat_new,
view_new_second_level_cat_new,
view_same_third_level_cat_new,
view_new_third_level_cat_new,
view_same_taxonomy_id,
view_new_taxonomy_id,
count(user_id) AS user_count
FROM candidate
GROUP BY 1,2,3,4,5,6,7,8
;

--======================--
--|Show Purchase Signal|--
--======================--
-- How many percent of users show a purchase signal before making a purchase? Whether buyers viewed the category purchased in the past 3 months before purchase? 



--=====================================================--
--|TIME SPENT ON PAGE BETWEEN LANDING AND SECOND EVENT|--
--=====================================================--
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.page_event_time_spent` AS (
SELECT  
a.visit_id,
converted,
CASE WHEN v.user_id IS NOT NULL THEN 1 ELSE 0 END AS sign_in_flag,
a.landing_page_type,
b.event_type,
EXTRACT(DATE FROM timestamp_millis(b.epoch_ms)) AS visit_day,
time(timestamp_millis(b.epoch_ms)) as event_start_time,
rank() over (PARTITION BY a.visit_id,a.landing_page_type, EXTRACT(DATE FROM timestamp_millis(b.epoch_ms)) ORDER BY b.epoch_ms ASC) AS sequence_number,
-- lead(b.epoch_ms) over (PARTITION BY a.visit_id ORDER BY b.epoch_ms ASC) as page_view_time
FROM `etsy-visit-pipe-prod.canonical.visits_recent` a
JOIN UNNEST(events.events_tuple) b
JOIN `etsy-data-warehouse-prod.weblog.visits` v USING (visit_id)
WHERE _PARTITIONDATE BETWEEN '2022-08-01' AND '2022-08-07' 
AND _DATE BETWEEN '2022-08-01' AND '2022-08-07'
AND a.is_bounce = FALSE
AND a.is_possible_bot = FALSE
AND a.is_admin = FALSE
AND a.is_test_account = FALSE
AND a.event_source = 'web'
AND b.is_primary = TRUE --look for primary event (CLICKING)
AND cast(a.user_id as INTEGER) NOT IN (SELECT user_id FROM `etsy-data-warehouse-prod.rollups.seller_basics`)
)
;

-- PERCENTILE OF TIME SPENT ON PAGE
SELECT 
a.landing_page_type,
b.event_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2
ORDER BY 1,2
;

-- PERCENTILE OF TIME SPENT ON PAGE (CONVERT VS NON CONVERT)
SELECT 
a.converted,
a.landing_page_type,
b.event_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- PERCENTILE OF TIME SPENT ON PAGE (SIGN-IN VS NON SIGN-IN)
SELECT 
a.sign_in_flag,
a.landing_page_type,
b.event_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- AVERAGE TIME SPENT BETWEEN LANDING AND SECOND EVENT OF INTEREST
SELECT 
a.landing_page_type,
b.event_type AS second_event_type,
count(*) as visit_count,
avg(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND)) as average_time_spent
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2
ORDER BY 1,2
;

-- AVERAGE TIME SPENT BETWEEN LANDING AND SECOND EVENT OF INTEREST (CONVERT VS NON CONVERT)
SELECT 
a.converted,
a.landing_page_type,
b.event_type AS second_event_type,
count(*) as visit_count,
avg(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND)) as average_time_spent
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- AVERAGE TIME SPENT BETWEEN LANDING AND SECOND EVENT OF INTEREST (SIGN-IN VS NON SIGN-IN)
SELECT 
a.sign_in_flag,
a.landing_page_type,
b.event_type AS second_event_type,
count(*) as visit_count,
avg(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND)) as average_time_spent
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND b.event_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- REGARDLESS OF WHAT THE SECOND EVENT IS
-- TIME SPENT BETWEEN LANDING AND SECOND EVENT
SELECT 
a.sign_in_flag,
a.landing_page_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 2
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2
ORDER BY 1,2
;

-- TIME SPENT BETWEEN LANDING AND THIRD EVENT
SELECT 
a.sign_in_flag,
a.landing_page_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 3
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2
ORDER BY 1,2
;

-- TIME SPENT BETWEEN LANDING AND FORTH EVENT
SELECT 
a.sign_in_flag,
a.landing_page_type,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(1)] AS percentile_1,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(10)] AS percentile_10,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_20,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_30,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(20)] AS percentile_40,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(50)] AS percentile_50,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(75)] AS percentile_75,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(99)] AS percentile_99,
APPROX_QUANTILES(TIME_DIFF(b.event_start_time, a.event_start_time, SECOND), 100)[OFFSET(100)] AS percentile_100,
FROM `etsy-data-warehouse-dev.nlao.page_event_time_spent` a
JOIN `etsy-data-warehouse-dev.nlao.page_event_time_spent` b
   ON a.visit_id = b.visit_id AND a.visit_day = b.visit_day AND a.sequence_number = 1 AND b.sequence_number = 4
WHERE a.landing_page_type IN ('home','view_listing','search') 
   AND a.landing_page_type = a.event_type
GROUP BY 1,2
ORDER BY 1,2
;
