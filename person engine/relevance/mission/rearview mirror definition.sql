-- definition based on past purchase: https://github.com/etsy-dev/cburke/blob/main/perso_engine/rearview_measurement.sql

-- definition based on last listing view 
create or replace table `etsy-data-warehouse-dev.nlao.home_landings`
	as (
with events as (
select
rv.user_id,
rv.browser_id,
rv.visit_id,
rv.platform,
rv._date,
rv.start_datetime,
rv.converted,
rv.bounced,
rv.total_gms,
e.sequence_number,
row_number() over (partition by e.visit_id order by e.sequence_number) as page_seq
from `etsy-data-warehouse-prod.weblog.events` e
join `etsy-data-warehouse-prod.weblog.recent_visits` rv using (visit_id)
where rv._date between "2023-11-11" and "2023-12-10"
and rv.landing_event in ("home","homescreen")
and e.page_view = 1)
select
user_id,
browser_id,
visit_id,
platform,
_date,
start_datetime,
converted,
bounced,
total_gms, 
max(case when page_seq = 2 then sequence_number else null end) as second_page_seq
from events
group by 1,2,3,4,5,6,7,8,9);

create or replace table `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions`
as (
select  
	r.visit_id,
	v.second_page_seq,
	v._date,
	r.platform,
	v.browser_id,
	v.user_id,
	v.converted,
	v.bounced,
	r.buyer_segment,
	r.module_page,
	r.module_placement,
	r.listing_id,
	r.rec_taxonomy_id,
	r.rec_price,
	t.full_path,
	t.top_level_cat,
	t.second_level_cat,
	t.third_level_cat,
	count(*) as delivered_count,
	sum(seen) as seen_count,
	sum(clicked) as clicked_count,
	sum(purchased_after_view) as purchased_count,
	sum(transactions_quantity) as transactions_quantity,
	sum(transactions_gms) as transactions_gms
	from `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
	join `etsy-data-warehouse-dev.nlao.home_landings` v on r.visit_id = v.visit_id 
	join `etsy-data-warehouse-prod.materialized.taxonomy_features` t on r.rec_taxonomy_id = t.taxonomy_id
	where r._date between "2023-11-11" and "2023-12-10"
	and (r.sequence_number < v.second_page_seq or v.second_page_seq is null) --recs on homepage landings
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18);

create or replace table `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing`
as (
with signed_out as (
select
h.*,
a.listing_id,
a.taxonomy_id,
t.full_path,
t.top_level_cat,
t.second_level_cat,
t.third_level_cat,
timestamp_millis(l.epoch_ms) AS ts_ms,
row_number() over (partition by h.visit_id order by timestamp_millis(l.epoch_ms) desc) as listing_view_sequence
from `etsy-data-warehouse-dev.nlao.home_landings` h
join `etsy-data-warehouse-prod.analytics.listing_views` l
    ON (split(l.visit_id,  ".")[ORDINAL(1)]) = h.browser_id and l.visit_id < h.visit_id
join `etsy-data-warehouse-prod.listing_mart.listing_attributes` a
  ON l.listing_id = a.listing_id
join `etsy-data-warehouse-prod.materialized.taxonomy_features` t
  ON a.taxonomy_id = t.taxonomy_id
where h.user_id is null
AND l._date < current_date
),
signed_in as (
select
h.*,
a.listing_id,
a.taxonomy_id,
t.full_path,
t.top_level_cat,
t.second_level_cat,
t.third_level_cat,
timestamp_millis(l.epoch_ms) AS ts_ms,
row_number() over (partition by h.visit_id order by timestamp_millis(l.epoch_ms) desc) as listing_view_sequence
from `etsy-data-warehouse-dev.nlao.home_landings` h
join `etsy-data-warehouse-prod.weblog.visits` v
  ON h.user_id = v.user_id and v.start_datetime < h.start_datetime
join `etsy-data-warehouse-prod.analytics.listing_views` l
  ON v.visit_id = l.visit_id
join `etsy-data-warehouse-prod.listing_mart.listing_attributes` a
  ON l.listing_id = a.listing_id
join `etsy-data-warehouse-prod.materialized.taxonomy_features` t
  ON a.taxonomy_id = t.taxonomy_id
where h.user_id is not null
AND v._date < current_date
AND l._date < current_date
)
select
* 
from signed_out
where listing_view_sequence = 1
union all
select *
from signed_in
where listing_view_sequence = 1);

select
case when h.user_id is not null then 1 else 0 end as signed_in,
CASE WHEN date_diff(h._date,date(ts_ms),day)>=0 then 1 
WHEN date_diff(h._date,date(ts_ms),day)<0 then 0
else null end as greater_than_0,
count(*) as ct
from `etsy-data-warehouse-dev.nlao.home_landings` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` using (visit_id)
group by 1,2
;

select
case when ts_ms is not null then 1 else 0 end as has_past_listing_view,
count(distinct h.visit_id)
from `etsy-data-warehouse-dev.nlao.home_landings` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` using (visit_id)
group by 1
order by 1;

select
case when h.user_id is not null then 1 else 0 end as signed_in,
case when ts_ms is not null then 1 else 0 end as has_past_listing_view,
count(distinct h.visit_id)
from `etsy-data-warehouse-dev.nlao.home_landings` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` using (visit_id)
group by 1,2
order by 1,2;

select case when buyer_segment = "Not Active" then buyer_segment else "Active" end as buyer_segment,
case when ts_ms is not null then 1 else 0 end as has_past_listing_view,
count(distinct h.visit_id)
from `etsy-data-warehouse-dev.nlao.home_landings` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` using (visit_id)
join `etsy-data-warehouse-prod.user_mart.mapped_user_profile` on h.user_id = mapped_user_id
group by 1,2
order by 3 desc;


with base as (select
h.visit_id,
h.user_id,
date_diff(h._date,date(ts_ms),day) as days_since_last_listing_view
from `etsy-data-warehouse-dev.nlao.home_landings` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id))
select 
case when days_since_last_listing_view is null then null
when days_since_last_listing_view <= 1 then "a. 0-1"
when days_since_last_listing_view <= 7 then "b. 2-7"
when days_since_last_listing_view <= 14 then "c. 8-14"
when days_since_last_listing_view <= 30 then "d. 15-30"
when days_since_last_listing_view <= 60 then "e. 31-60"
when days_since_last_listing_view <= 90 then "f. 61-90"
when days_since_last_listing_view <= 180 then "g. 91-180"
when days_since_last_listing_view <= 365 then "h. 181-365"
when days_since_last_listing_view > 365 then "i. 365+" end as days_since_last_listing_view,
count(*)
from base
group by 1
order by 1;

--% of homepage recs that are same listing, same taxo, same top level as the user's last listing view
with base as (
select
buyer_segment,
h.visit_id,
h.module_placement,
h.listing_id,
h.rec_taxonomy_id,
h.top_level_cat,
h.second_level_cat,
l.listing_id as last_view_listing_id,
l.taxonomy_id as last_view_taxonomy_id,
l.top_level_cat as last_view_top_level_cat,
l.second_level_cat as last_view_second_level_cat
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id)
)
select
buyer_segment,
case when last_view_taxonomy_id is null then "1. no past view"
when last_view_listing_id = listing_id then "2. same listing_id"
when last_view_taxonomy_id = rec_taxonomy_id then "3. same taxonomy_id"
when last_view_second_level_cat = second_level_cat and last_view_top_level_cat = top_level_cat then "4. same second_level_cat" --accounts for cases where same subcat exists in different top categories 
when last_view_top_level_cat = top_level_cat then "5. same top_level_cat" 
else "6. other" end as match_type,
count(*) as recs_impressions
from base
group by 1,2 order by 1;

--Visit level average percent
with base as (
select
h.visit_id,
h.converted,
h.bounced,
h.module_placement,
h.listing_id,
h.rec_taxonomy_id,
h.top_level_cat,
h.second_level_cat,
l.listing_id as last_view_listing_id,
l.taxonomy_id as last_view_taxonomy_id,
l.top_level_cat as last_view_top_level_cat,
l.second_level_cat as last_view_second_level_cat,
h.delivered_count,
h.seen_count,
h.clicked_count,
h.purchased_count
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id)
),
visit_level as (
select
visit_id,
converted,
bounced,
sum(case when last_view_listing_id = listing_id then 1 else 0 end)/count(*) as pct_same_listing,
sum(case when  last_view_taxonomy_id = rec_taxonomy_id  then 1 else 0 end)/count(*) as pct_same_taxonomy,
sum(case when last_view_second_level_cat = second_level_cat and last_view_top_level_cat = top_level_cat then 1 else 0 end)/count(*) as pct_same_subcat,
sum(case when last_view_top_level_cat = top_level_cat  then 1 else 0 end)/count(*) as pct_same_top_level,
from base
where last_view_listing_id is not null
group by 1,2,3)
select 
avg(pct_same_listing) as avg_pct_same_listing,
avg(pct_same_taxonomy) as avg_pct_same_taxonomy,
avg(pct_same_subcat) as avg_pct_same_subcat,
avg(pct_same_top_level) as avg_pct_same_top_level
from visit_level;

--Distribution of % per visit
with base as (
select
h.visit_id,
h.converted,
h.bounced,
h.module_placement,
h.listing_id,
h.rec_taxonomy_id,
h.top_level_cat,
h.second_level_cat,
l.listing_id as last_viewed_listing_id,
l.taxonomy_id as last_viewed_taxonomy_id,
l.top_level_cat as last_viewed_top_level_cat,
l.second_level_cat as last_viewed_second_level_cat,
h.delivered_count,
h.seen_count,
h.clicked_count,
h.purchased_count
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id)
),
visit_level as (
select
visit_id,
converted,
bounced,
sum(case when last_viewed_listing_id = listing_id then 1 else 0 end)/count(*) as pct_same_listing,
sum(case when  last_viewed_taxonomy_id = rec_taxonomy_id  then 1 else 0 end)/count(*) as pct_same_taxonomy,
sum(case when last_viewed_second_level_cat = second_level_cat and last_viewed_top_level_cat = top_level_cat then 1 else 0 end)/count(*) as pct_same_subcat,
sum(case when last_viewed_top_level_cat = top_level_cat  then 1 else 0 end)/count(*) as pct_same_top_level,
from base
where last_viewed_listing_id is not null
group by 1,2,3)
select 
case when pct_same_subcat = 0 then "a. 0%"
when pct_same_subcat between 0 and 0.1 then "b. 1-10%"
when pct_same_subcat between 0.1 and 0.2 then "c. 10-20%"
when pct_same_subcat between 0.2 and 0.3 then "d. 20-30%"
when pct_same_subcat between 0.2 and 0.3 then "e. 20-30%"
when pct_same_subcat between 0.3 and 0.4 then "f. 30-40%"
when pct_same_subcat between 0.4 and 0.5 then "g. 40-50%"
when pct_same_subcat between 0.5 and 0.6 then "h. 50-60%"
when pct_same_subcat between 0.6 and 0.7 then "i. 60-70%"
when pct_same_subcat between 0.7 and 0.8 then "j. 20-80%"
when pct_same_subcat between 0.8 and 0.9 then "k. 80-90%"
when pct_same_subcat between 0.9 and 1 then "l. 90-100%" end as pct_same_subcat,
--round(pct_same_taxonomy,2) as pct_same_taxonomy,
--round(pct_same_subcat,2) as pct_same_subcat,
--round(pct_same_top_level,2) as pct_same_top_level,
count(*) as visits,
sum(converted) as converted_visits,
sum(bounced) as bounced_visits
from visit_level group by 1 order by 1;

--stats for visits w no past purchase
with base as (
select
h.visit_id,
h.converted,
h.bounced,
h.module_placement,
h.listing_id,
h.rec_taxonomy_id,
h.top_level_cat,
h.second_level_cat,
l.listing_id as last_viewed_listing_id,
l.taxonomy_id as last_viewed_taxonomy_id,
l.top_level_cat as last_viewed_top_level_cat,
l.second_level_cat as last_viewed_second_level_cat,
h.delivered_count,
h.seen_count,
h.clicked_count,
h.purchased_count
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id)
),
visit_level as (
select
distinct
visit_id,
converted,
bounced
from base
where last_viewed_listing_id is  null
group by 1,2,3)
select
"no past listing view" as pct_same_subcat,
count(*) as visits,
sum(converted) as converted_visits,
sum(bounced) as bounced_visits
from visit_level group by 1 order by 1;

--Time since last listing view
with base as (select
h.visit_id,
date_diff(h._date,date(ts_ms),day) as days_since_last_listing_view,
case when l.listing_id = h.listing_id then 1 else 0 end as same_listing,
case when  l.taxonomy_id = h.rec_taxonomy_id  then 1 else 0 end as same_taxonomy,
case when l.second_level_cat = h.second_level_cat and l.top_level_cat = h.top_level_cat then 1 else 0 end as same_subcat,
case when l.top_level_cat = h.top_level_cat  then 1 else 0 end as same_top_level,
h.listing_id,
h.seen_count,
h.clicked_count,
h.purchased_count
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id))
select 
case when days_since_last_listing_view is null then null
when days_since_last_listing_view <= 365 then days_since_last_listing_view
when days_since_last_listing_view > 365 then 366 end as days_since_last_listing_view,
sum(case when same_listing = 1 then seen_count else null end) as same_listing_seen,
sum(case when same_listing = 1 then clicked_count else null end) as same_listing_clicked,
sum(case when same_listing = 1 then purchased_count else null end) as same_listing_purchased,
sum(case when same_taxonomy = 1 then seen_count else null end) as same_taxonomy_seen,
sum(case when same_taxonomy = 1 then clicked_count else null end) as same_taxonomy_clicked,
sum(case when same_taxonomy = 1 then purchased_count else null end) as same_taxonomy_purchased,
sum(case when same_subcat = 1 then seen_count else null end) as same_subcat_seen,
sum(case when same_subcat = 1 then clicked_count else null end) as same_subcat_clicked,
sum(case when same_subcat = 1 then purchased_count else null end) as same_subcat_purchased,
sum(case when same_top_level = 1 then seen_count else null end) as same_top_level_seen,
sum(case when same_top_level = 1 then clicked_count else null end) as same_top_level_clicked,
sum(case when same_top_level = 1 then purchased_count else null end) as same_top_level_purchased,
from base
group by 1
order by 1;

with base as (
select
h.visit_id,
date_diff(h._date,date(ts_ms),day) as days_since_last_listing_view,
h.bounced,
count(*) as total_imp,
sum(case when l.listing_id = h.listing_id then 1 else 0 end) as same_listing,
sum(case when  l.taxonomy_id = h.rec_taxonomy_id  then 1 else 0 end) as same_taxonomy,
sum(case when l.second_level_cat = h.second_level_cat and l.top_level_cat = h.top_level_cat then 1 else 0 end) as same_subcat,
sum(case when l.top_level_cat = h.top_level_cat  then 1 else 0 end) as same_top_level
from `etsy-data-warehouse-dev.nlao.home_landing_listing_impressions` h
left join `etsy-data-warehouse-dev.nlao.home_landings_last_view_listing` l using (visit_id)
group by 1,2,3)
select 
case when days_since_last_listing_view is null then null
when days_since_last_listing_view <= 365 then days_since_last_listing_view
when days_since_last_listing_view > 365 then 366 end as days_since_last_listing_view,
sum(total_imp) as total_imp,
sum(same_listing)/sum(total_imp) as pct_same_listing,
sum(same_taxonomy)/sum(total_imp) as pct_same_taxo,
sum(same_subcat)/sum(total_imp) as pct_same_subcat,
sum(same_top_level)/sum(total_imp) as pct_same_top_level
from base
group by 1
order by 1;

