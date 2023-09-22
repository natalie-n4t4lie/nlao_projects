-- ## TREATMENT VISITS ##--
-- double check experiment volume 
select
_DATE,
COUNT(*)
from `etsy-data-warehouse-prod.catapult.ab_tests`
where ab_test = "intl_growth.lp_complete_the_look_recs_module.v1-1"
AND _DATE >= '2022-10-01'
GROUP BY 1
ORDER BY 1 ASC
;-- steady volume BETWEEN "2022-10-20" AND "2022-11-11"

-- get taxonomy for listings detected by CTL model
create or replace table `etsy-data-warehouse-dev.nlao.ctl_listings`
	as (
select distinct 
a.listing_id,
b.taxonomy_id,
c.full_path,
split(c.full_path, ".")[safe_offset(0)] as listing_top_cat,
split(c.full_path, ".")[safe_offset(1)] as listing_second_cat  
from `etsy-data-warehouse-prod.computer_vision.shop_w_etsy-ctl_detected_products` a
join `etsy-data-warehouse-prod.listing_mart.listing_attributes` b using (listing_id)
join `etsy-data-warehouse-prod.structured_data.taxonomy_latest` c using (taxonomy_id))
;-- 13,224,208 listings

-- get listing views stats (visit_id) for detected listings
create or replace table `etsy-data-warehouse-dev.nlao.ctl_listing_views`
	as (
select
lv.*,
ctl.taxonomy_id,
ctl.full_path,
ctl.listing_top_cat,
ctl.listing_second_cat
from `etsy-data-warehouse-prod.analytics.listing_views` lv
join `etsy-data-warehouse-dev.nlao.ctl_listings`ctl using (listing_id)
where lv._date BETWEEN "2022-10-20" AND "2022-11-11")
;-- 183,974,309 listing views

-- get bucketing for eligible visits
create or replace table `etsy-data-warehouse-dev.nlao.browsers_with_event_ctl_d`
as (
select
a.ab_variant,
(split(a.visit_id,  ".")[ORDINAL(1)]) as browser_id,
min(a.run_date) as bucket_run_date,
min(a.visit_id) as bucket_visit_id
from  `etsy-data-warehouse-prod.catapult.ab_tests` a
join `etsy-data-warehouse-dev.nlao.ctl_listing_views` m 
on a.visit_id = m.visit_id
and m.sequence_number >= a.sequence_number
where a.ab_test = "intl_growth.lp_complete_the_look_recs_module.v1-1"
and a._date BETWEEN "2022-10-20" AND "2022-11-11"
and m._date BETWEEN "2022-10-20" AND "2022-11-11"
group by 1,2
order by 1,2)
;

-- for eligible visits find those scolled down to recs module, the real treatment
create or replace table `etsy-data-warehouse-dev.nlao.ctl_module_seen` as
with events as (
SELECT
    a.visit_id,
    c.ab_variant,
		(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "module_placement" ) AS recs_module,
		(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "epoch_ms") as epoch_ms
FROM
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` as a
INNER JOIN `etsy-data-warehouse-dev.nlao.browsers_with_event_ctl_d` c on a.visit_id = c.bucket_visit_id
WHERE beacon.event_name = 'recommendations_module_seen'
AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "module_placement" ) = "listing_page_complete_the_look_recs"
)
select
visit_id,
ab_variant,
recs_module,
min(epoch_ms) as min_epoch,
count(*) as event_count
from events
group by 1,2,3
;

-- DETAILS
select
beacon.event_name,
(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "module_placement" ) AS recs_module,
(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "target_listing_id" ) AS target_listing_id,
(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "listing_ids" ) AS listing_ids,
(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "listing_prices_usd" ) AS listing_prices_usd,
(SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "taxonomy_ids" ) AS taxonomy_ids,
FROM
  `etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where 	
beacon.event_name = 'recommendations_module_seen'
AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "module_placement" ) = "listing_page_complete_the_look_recs"
limit 10
;

SELECT max(_PARTITIONTIME), min(_PARTITIONTIME) FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` WHERE DATE(_PARTITIONTIME) <= "2022-12-20" LIMIT 1000
;--past 30 days
--2022-12-20 14:00:00 UTC
-- 2022-11-19 17:00:00 UTC


