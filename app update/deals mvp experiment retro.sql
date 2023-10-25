/*
This script contains queries on the catapult_unified tables that can be used for analyzing experiment results.
It takes in an experiment (config flag) and date range as input, and provides the following data:
* First bucketing moment for each experimental unit (i.e. browser or user)
* Segment values for each experimental unit
* Event counts, GMS, and visit count for each experimental unit
* Combined data for all events and experimental units, with segment values
* Query to recreate the metric results from the Catapult UI

If you want to join catapult data with non-catapult tables that rely on visit ids,
this script also includes example queries on how to get visit ids for your experiment.

For more details on how to work with catapult_unified tables, go to go/catapult-unified-docs
*/

-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING default "mobile_dynamic_config.iphone.BOENewDealsTab";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify a custom date range, you can also specify the start and end date manually.
DECLARE start_date DATE; -- default "2023-08-22";
DECLARE end_date DATE; -- default "2023-09-04";

SET (start_date, end_date) = (
    select as struct
        max(date(boundary_start_ts)) as start_date,
        max(_date) as end_date
    from `etsy-data-warehouse-prod.catapult_unified.experiment`
    where experiment_id = config_flag_param
);

-- Tip: Replace 'nlao' in the table names below with your own username or personal dataset name.

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (i.e. browser or user).
-- Note: for event filtered experiments, swap the 'bucketing_ts' with 'filtered_bucketing_ts'
-- to get the event filtered bucketing moment.
create or replace table `etsy-data-warehouse-dev.nlao.ab_first_bucket` as (
select
  bucketing_id,
  variant_id,
  bucketing_ts,
from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
where _date = end_date
and experiment_id = config_flag_param
);

create or replace table `etsy-data-warehouse-dev.nlao.subsequent_visits` as (
    select
        b.bucketing_id,
        b.variant_id,
        v.visit_id,
        rank() over (partition by bucketing_id order by v.start_datetime) AS visit_sequence
    from `etsy-data-warehouse-dev.nlao.ab_first_bucket` b
    join `etsy-data-warehouse-prod.weblog.visits` v
    -- swap the browser_id for user_id in the case of a user experiment
    on b.bucketing_id = v.browser_id
    and timestamp_trunc(bucketing_ts, SECOND) <= v.end_datetime
    where v._date between start_date and end_date
);

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- Get segment values based on first bucketing moment.
create or replace table `etsy-data-warehouse-dev.nlao.first_bucket_segments` as (
-- Here you can specify as many segmentations as you'd like using the event_id.
-- For all supported segmentations, see go/catapult-unified-docs
with buyer_segments as (
    select
        bucketing_id,
        variant_id,
        event_value
    from `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
    where _date = end_date
    and experiment_id = config_flag_param
    and event_id = "buyer_segment"
),
regions as (
    select
        bucketing_id,
        variant_id,
        event_value
    from `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
    where _date = end_date
    and experiment_id = config_flag_param
    and event_id = "canonical_region"
)
select
    bucketing_id,
    variant_id,
    seg1.event_value as buyer_segment,
    seg2.event_value as region
from buyer_segments seg1
join regions seg2
using (bucketing_id, variant_id)
);

-------------------------------------------------------------------------------------------
-- EVENT DATA
-------------------------------------------------------------------------------------------
-- Specify the events you want to analyze here.
create or replace table `etsy-data-warehouse-dev.nlao.events` as (
  select * from unnest([
    "backend_cart_payment", -- conversion rate
    "total_winsorized_gms", -- winsorized acbv
    "prolist_total_spend" -- prolist revenue
  ]) as event_id
);

-- Get all the bucketed units with the events of interest.
-- Note: for event filtered experiments, swap the 'event_value' with 'filtered_event_value'
-- to get event filtered results.
create or replace table `etsy-data-warehouse-dev.nlao.events_per_unit` as (
    select
        bucketing_id,
        variant_id,
        event_id,
        event_value
    from `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(start_date, end_date)
    join `etsy-data-warehouse-dev.nlao.events` using (event_id)
    where experiment_id = config_flag_param
    and event_type in (1, 2) -- fired event or custom event (see go/catapult-unified-enums)
);
-------------------------------------------------------------------------------------------
-- GMS DATA
-------------------------------------------------------------------------------------------
create or replace table `etsy-data-warehouse-dev.nlao.gms_per_unit` as (
    select
      bucketing_id,
      variant_id,
      event_value as gms -- note: gms data is in cents
    from `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(start_date, end_date)
    where experiment_id = config_flag_param
    and event_id = "gms"
    and event_type = 3
);
-------------------------------------------------------------------------------------------
-- VISIT COUNT
-------------------------------------------------------------------------------------------

-- Get all post-bucketing visits for each experimental unit
create or replace table `etsy-data-warehouse-dev.nlao.subsequent_visits` as (
    select
        b.bucketing_id,
        b.variant_id,
        v.visit_id,
        rank() over (partition by bucketing_id order by start_datetime ASC) AS visit_sequence
    from `etsy-data-warehouse-dev.nlao.ab_first_bucket` b
    join `etsy-data-warehouse-prod.weblog.visits` v
    -- swap the browser_id for user_id in the case of a user experiment
    on b.bucketing_id = v.browser_id
    and timestamp_trunc(bucketing_ts, SECOND) <= v.end_datetime
    where v._date between start_date and end_date
);

-- Get visit count per experimental unit
create or replace table `etsy-data-warehouse-dev.nlao.visits_per_unit` as (
    select
      bucketing_id,
      variant_id,
      count(*) as visit_count
    from `etsy-data-warehouse-dev.nlao.subsequent_visits`
    group by bucketing_id, variant_id
);

-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA
-------------------------------------------------------------------------------------------
-- All events for all bucketed units, with segment values.
create or replace table `etsy-data-warehouse-dev.nlao.all_units_events_segments` as (
    select
      bucketing_id,
      variant_id,
      event_id,
      coalesce(event_value, 0) as event_count,
      buyer_segment,
      region
    from `etsy-data-warehouse-dev.nlao.ab_first_bucket`
    cross join `etsy-data-warehouse-dev.nlao.events`
    left join `etsy-data-warehouse-dev.nlao.events_per_unit`
      using (bucketing_id, variant_id, event_id)
    join `etsy-data-warehouse-dev.nlao.first_bucket_segments`
      using (bucketing_id, variant_id)
);

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS
-------------------------------------------------------------------------------------------
-- Percent and mean metrics by variant and event_name
select
    event_id,
    variant_id,
    count(*) as total_units_in_variant,
    avg(if(event_count = 0, 0, 1)) as percent_units_with_event,
    avg(event_count) as avg_events_per_unit,
    avg(if(event_count = 0, NULL, event_count)) as avg_events_per_unit_with_event
from `etsy-data-warehouse-dev.nlao.all_units_events_segments`
group by event_id, variant_id
order by event_id, variant_id
;

-------------------------------------------------------------------------------------------
-- VISIT IDS TO JOIN WITH EXTERNAL TABLES
-------------------------------------------------------------------------------------------
-- Need visit ids to join with non-catapult tables?
-- No problem! Here are some examples for how to get the visit ids for each experimental unit.

-- All bucket visit ids
create or replace table `etsy-data-warehouse-dev.nlao.ab_first_bucket` as (
with bucket_visits as (
  select
      bucketing_id,
      variant_id,
      bucketing_ts,
      ids.id as visit_id
  from `etsy-data-warehouse-prod.catapult_unified.bucketing`
  cross join unnest(associated_ids) ids
  where _date between start_date and end_date
  and experiment_id = config_flag_param
  and ids.id_type = 3 -- visit_id
)
select
  bucketing_id,
  variant_id,
  bucketing_ts,
  visit_id bucket_visit_id
from `etsy-data-warehouse-prod.catapult_unified.bucketing_period` fb
join bucket_visits using (bucketing_id, variant_id, bucketing_ts)
where fb._date = end_date
and experiment_id = config_flag_param
);

------------------------------Natalie's adhoc code

-- visit frequency and visit day count
WITH cte AS (
    select
        b.bucketing_id,
        b.variant_id,
        count(distinct b.visit_id) as visit_count,
        count(distinct v._date) AS visit_day_ct
    from `etsy-data-warehouse-dev.nlao.subsequent_visits` b
    join `etsy-data-warehouse-prod.weblog.visits` v
    -- swap the browser_id for user_id in the case of a user experiment
    on b.visit_id = v.visit_id
    where v._date between '2023-10-13' AND '2023-10-23'
    GROUP BY 1,2
)
SELECT
variant_id,
CASE WHEN visit_count > 10 then 10 else visit_count end as visit_count_grouping,
visit_day_ct,
count(bucketing_id) AS browser_ct
FROM cte
GROUP BY 1,2,3
;

--visit frequency: mean and percentile
SELECT DISTINCT
variant_id,
avg(visit_count) OVER (PARTITION BY variant_id) AS mean_visit_count,
PERCENTILE_CONT(visit_count, 0.01) OVER (PARTITION BY variant_id) AS percentile_1,
PERCENTILE_CONT(visit_count, 0.1) OVER (PARTITION BY variant_id) AS percentile_10,
PERCENTILE_CONT(visit_count, 0.2) OVER (PARTITION BY variant_id) AS percentile_20,
PERCENTILE_CONT(visit_count, 0.3) OVER (PARTITION BY variant_id) AS percentile_30,
PERCENTILE_CONT(visit_count, 0.4) OVER (PARTITION BY variant_id) AS percentile_40,
PERCENTILE_CONT(visit_count, 0.5) OVER (PARTITION BY variant_id) AS percentile_50,
PERCENTILE_CONT(visit_count, 0.5) OVER (PARTITION BY variant_id) AS percentile_60,
PERCENTILE_CONT(visit_count, 0.7) OVER (PARTITION BY variant_id) AS percentile_70,
PERCENTILE_CONT(visit_count, 0.8) OVER (PARTITION BY variant_id) AS percentile_80,
PERCENTILE_CONT(visit_count, 0.9) OVER (PARTITION BY variant_id) AS percentile_90,
PERCENTILE_CONT(visit_count, 0.99) OVER (PARTITION BY variant_id) AS percentile_99,
PERCENTILE_CONT(visit_count, 0.999) OVER (PARTITION BY variant_id) AS percentile_999
FROM `etsy-data-warehouse-dev.nlao.visits_per_unit`
;

-- visit day & visit count distribution
WITH cte AS (
    select
        b.bucketing_id,
        b.variant_id,
        count(distinct b.visit_id) as visit_count,
        count(distinct v._date) AS visit_day_ct
    from `etsy-data-warehouse-dev.nlao.subsequent_visits` b
    join `etsy-data-warehouse-prod.weblog.visits` v
    -- swap the browser_id for user_id in the case of a user experiment
    on b.visit_id = v.visit_id
    where v._date between '2023-10-13' AND '2023-10-23'
    GROUP BY 1,2
)
SELECT
variant_id,
CASE WHEN visit_count > 10 then 10 else visit_count end as visit_count_grouping,
visit_day_ct,
count(bucketing_id) AS browser_ct
FROM cte
GROUP BY 1,2,3
;

--- visit by channel
SELECT
wv.top_channel,
variant_id,
count(visit_id) AS visit_ct
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.weblog.visits` wv USING (visit_id)
WHERE wv._date BETWEEN '2023-10-18' AND '2023-10-23' AND visit_sequence>1
GROUP BY 1,2
;

SELECT
wv.start_reason,
variant_id,
count(visit_id) AS visit_ct
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.weblog.visits` wv USING (visit_id)
WHERE wv._date BETWEEN '2023-10-18' AND '2023-10-23'
GROUP BY 1,2
;

-- tab engagment - visit/browser level
WITH cte AS (
SELECT
    variant_id,
    -- visit_id,
    bucketing_id,
    MAX(CASE WHEN event_type IN ("in_app_notifications","deals_tab_delivered") THEN 1 ELSE 0 END) AS visit_updates,
		MAX(CASE WHEN event_type IN ("home","homescreen") THEN 1 ELSE 0 END) AS home_tab,
		-- MAX(CASE WHEN event_type IN ("search") THEN 1 ELSE 0 END) AS search,
		MAX(CASE WHEN event_type IN ("favorites","favorites_and_lists","profile_favorite_listings_tab") THEN 1 ELSE 0 END) AS favorite_tab,
		MAX(CASE WHEN event_type IN ("cart_view") THEN 1 ELSE 0 END) AS  cart_tab,
		MAX(CASE WHEN event_type IN ("you_screen","you_tab_viewed") THEN 1 ELSE 0 END) AS you_tab,
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id)
-- WHERE visit_sequence >1 -- comment in when look for subsequent visit
GROUP BY 1,2
)
SELECT
variant_id,
-- count(visit_id) AS total_visit,
count(bucketing_id) AS total_browser,
sum(visit_updates) AS visit_updates,
sum(home_tab) AS home_tab,
sum(favorite_tab) AS favorite_tab,
sum(cart_tab) AS cart_tab,
sum(you_tab) AS you_tab
FROM cte
GROUP BY 1
;

-- for those who converted, what's the average transactions ct and purchase day?
WITH CTE AS (
SELECT
variant_id,
bucketing_id,
COUNT(DISTINCT transaction_id) AS transaction_ct,
COUNT(DISTINCT _date) AS purchase_date_ct
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits`
JOIN `etsy-data-warehouse-prod.visit_mart.visits_transactions` USING (visit_id)
GROUP BY 1,2
)
SELECT
variant_id,
avg(transaction_ct) as avg_transaction_ct,
avg(purchase_date_ct) as avg_purchase_date_ct
FROM cte
GROUP BY 1
;

-- What does user do in subsequent visit
WITH cte AS (
SELECT
    variant_id,
    -- bucketing_id,
    visit_id,
		MAX(CASE WHEN event_type IN ("shop_home") THEN 1 ELSE 0 END) AS  shop_home,
		MAX(CASE WHEN event_type IN ("view_listing") THEN 1 ELSE 0 END) AS  view_listing,
		MAX(CASE WHEN event_type IN ("search") THEN 1 ELSE 0 END) AS search,
		MAX(CASE WHEN event_type IN ("favorites","favorites_and_lists","profile_favorite_listings_tab") THEN 1 ELSE 0 END) AS favorites,
		MAX(CASE WHEN event_type IN ("favorites_tapped_list","collections_view") THEN 1 ELSE 0 END) AS  view_collection,
		MAX(CASE WHEN event_type IN ("cart_view") THEN 1 ELSE 0 END) AS  cart_view,
		MAX(CASE WHEN event_type IN ("view_receipt") THEN 1 ELSE 0 END) AS view_receipt,
		MAX(CASE WHEN event_type IN ("you_screen","you_tab_viewed") THEN 1 ELSE 0 END) AS you_tab,
		MAX(CASE WHEN event_type IN ("your_purchases","yr_purchases") THEN 1 ELSE 0 END) AS your_purchases,
		MAX(CASE WHEN event_type IN ("backend_send_convo") THEN 1 ELSE 0 END) AS send_message,
 		MAX(CASE WHEN event_type IN ("help_with_order_view") THEN 1 ELSE 0 END) AS help_with_order
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id)
WHERE V.visit_sequence > 1
GROUP BY 1,2
)
SELECT
variant_id,
count(bucketing_id) as total_browser,
count(visit_id) AS total_visit,
sum(shop_home) AS shop_home,
sum(view_listing) AS view_listing,
sum(search) AS search,
sum(favorites) AS favorites,
sum(view_collection) AS view_collection,
sum(cart_view) AS cart_view,
sum(view_receipt) AS view_receipt,
sum(you_tab) AS you_tab,
sum(your_purchases) AS your_purchases,
sum(send_message) AS send_message,
sum(help_with_order) AS help_with_order
FROM cte
GROUP BY 1
;

-- module view
SELECT
v.variant_id,
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(DISTINCT v.visit_id) AS visit_count
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` e USING (visit_id)
WHERE 
date(_partitiontime) BETWEEN '2023-10-18' AND '2023-10-23'
and beacon.event_name = "deals_tab_module_seen"
group by 1,2
;

-- module click
SELECT
v.variant_id,
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(DISTINCT v.visit_id) AS visit_count
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` e USING (visit_id)
WHERE 
date(_partitiontime) BETWEEN '2023-10-18' AND '2023-10-23'
AND beacon.event_name IN ("deals_tab_tapped_listing","deals_tab_tapped_shop","deals_tab_tapped_header","deals_tab_tapped_footer")
AND variant_id = 'on'
group by 1,2
;

SELECT
count(visit_id) AS visit_ct
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits`
WHERE variant_id = 'on'
;

