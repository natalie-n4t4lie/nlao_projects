-- Experiment retro - Transactional Landing
BEGIN 

-- *** ENTER BELOW *** --
DECLARE config_flag STRING default "beat.order_shipping_status_on_homescreen_v2.experiment"; -- enter experiment config flag
DECLARE start_date DATE default "2023-05-04"; -- enter experiment start date
DECLARE end_date DATE default "2023-05-14"; -- enter experiment end date

-- Bucketed visits for each variant during the experiment's timeframe
create or replace table `etsy-data-warehouse-dev.nlao.exp_visits` 
	as
	select 
		distinct 
		ab_test
		,ab_variant
		,visit_id
	from 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
	where 
		ab_test = config_flag
		and _date between start_date and end_date
;

END;

-- visit source
SELECT
referrer_type,
ab_variant,
count(visit_id) as visit_count
FROM `etsy-data-warehouse-dev.nlao.exp_visits` 
JOIN `etsy-data-warehouse-prod.weblog.visits` USING (visit_id)
WHERE _date between "2023-05-04" and "2023-05-14"
GROUP BY 1,2
;


-- exit event
SELECT
exit_event,
ab_variant,
count(visit_id) as visit_count
FROM `etsy-data-warehouse-dev.nlao.exp_visits` 
JOIN `etsy-data-warehouse-prod.weblog.visits` USING (visit_id)
WHERE _date between "2023-05-04" and "2023-05-14"
GROUP BY 1,2
;

-- view_receipt exit rate
with view_receipt_visits as (
  SELECT DISTINCT 
  v.ab_variant,
  v.visit_id, 
  rv.exit_event
  FROM `etsy-data-warehouse-dev.nlao.exp_visits` v
  JOIN `etsy-data-warehouse-prod.weblog.recent_visits` rv
    ON rv.visit_id = v.visit_id
  JOIN `etsy-data-warehouse-prod.weblog.events` e
    ON rv.visit_id = e.visit_id
    AND rv._date = e._date
  WHERE e._date between "2023-05-04" and "2023-05-14"
    AND rv._date between "2023-05-04" and "2023-05-14"
    AND e.event_type = "view_receipt"
)
SELECT ab_variant, 
count(*) as visit_count,
count(CASE WHEN exit_event = "view_receipt" THEN visit_id ELSE null END) as visit_exit_count,
AVG(CASE WHEN exit_event = "view_receipt" THEN 1 ELSE 0 END) as exit_rate
FROM view_receipt_visits
GROUP BY 1
;

-- deliver for real recs module below: modules below aren't actually recs modules, so their data won't show up in recs tables. they just show the user their recent activity. so we need to use the content of the boe_homescreen_tab_delivered event
    -- boe_homescreen_recent_favorites
    -- boe_homescreen_recently_viewed_horiz
  SELECT
    ab_variant,
    CONCAT("boe_homescreen_",module) as module,
    count(*) AS n_delivered
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  JOIN UNNEST(b.beacon.properties.key_value) as props
  JOIN UNNEST(JSON_VALUE_ARRAY(props.value)) module WITH OFFSET
  JOIN `etsy-data-warehouse-dev.nlao.exp_visits` v
  ON b.visit_id = v.visit_id
  WHERE DATE(_PARTITIONTIME) between "2023-05-04" and "2023-05-14"
  AND b.beacon.event_name='boe_homescreen_tab_delivered'
  AND props.key in ('modules')
  AND (
    module IN (
      "recently_viewed_horiz",
      "recent_favorites",
      "random_reminder_list",
      "our_picks",
      "similar_to_recently_viewed"
    )
  )
  GROUP BY 1,2
;

SELECT
  ab_variant,
  module_placement,
  count(*) as n_delivered
  FROM `etsy-data-warehouse-prod.analytics.recsys_delivered_modules` rm
  JOIN `etsy-data-warehouse-dev.nlao.exp_visits` v
  ON rm.visit_id = v.visit_id
where _date between "2023-05-04" and "2023-05-14"
AND module_placement IN ("boe_homescreen_our_picks","boe_homescreen_similar_to_recently_viewed")
GROUP BY 1, 2
;

-- TAPS
SELECT
    ab_variant,
    SPLIT(props.value, "-")[OFFSET(0)] as module,
    count(*) AS n_tapped
  FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  JOIN UNNEST(beacon.properties.key_value) as props
    ON props.key = "content_source"
  JOIN `etsy-data-warehouse-dev.nlao.exp_visits` v
  ON b.visit_id = v.visit_id
  WHERE  date(_partitiontime) between "2023-05-04" and "2023-05-14"
  AND beacon.event_name IN (
    'homescreen_tapped_listing',
    'homescreen_tapped_shop',
    'homescreen_tapped_search_term',
    'homescreen_tapped_list',
    "homescreen_tapped_view_all"
  )
  AND (props.value like "%boe_homescreen_our_picks%"
      OR props.value like"%boe_homescreen_recent_favorites%"
      OR props.value like"%boe_homescreen_recently_viewed_horiz%"
      OR props.value like"%boe_homescreen_similar_to_recently_viewed%"
      OR props.value like"%boe_homescreen_random_reminder_list%"
  ) 
  GROUP BY 1,2
;
