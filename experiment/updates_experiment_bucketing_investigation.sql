-- [iOS] Add Buy Now to “something in your cart is now on sale” Update
BEGIN 

-- *** ENTER BELOW *** --
DECLARE config_flag STRING default "deals_app.abandoned_cart_offer_with_strikethrough_original_price"; -- enter experiment config flag
DECLARE type_flag STRING default "shop_opportunities.abandoned_cart_updates.apps_experiment"; 
DECLARE start_date DATE default "2023-05-03"; -- enter experiment start date
DECLARE end_date DATE default "2023-05-15"; -- enter experiment end date

-- Bucketed visits for each variant during the experiment's timeframe
create temp table exp_visits
	as
	select 
		distinct 
    e.ab_test
    ,e.ab_variant
    ,value.list[OFFSET(0)].element as eligible
		,e.visit_id
    ,a.beacon.browser_id
    ,a.beacon.user_id,
    case when beacon.event_source in ('web', 'customshops', 'craft_web')
          and beacon.is_mobile_device IS FALSE then 'desktop'
          when beacon.event_source in ('web', 'customshops', 'craft_web')
          and beacon.is_mobile_device IS TRUE then  'mobile_web'
          when beacon.event_source in ('ios','android')
          and REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'soe'
          when beacon.event_source in ('ios')
          and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe ios'
          when beacon.event_source in ('android')
          and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe android'
          else 'undefined' end AS app_platform_case,
    case when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.29')) then '6.29'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.28')) then '6.28'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.27')) then '6.27'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.26')) then '6.26'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.25')) then '6.25'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.24')) then '6.24'
          else 'other' end AS app_version,
    a.sequence_number
	from 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
  JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
      ON a.visit_id = e.visit_id
    JOIN UNNEST(beacon.ab.key_value)
	where 
		e.ab_test = config_flag
		AND e._date between start_date and end_date
    AND DATE(_PARTITIONTIME) between start_date and end_date
    AND beacon.event_name = 'getting_eligible_notifications_for_feed'
    AND key = config_flag
;


create temp table exp_visits_type
	as
	select 
		distinct 
    e.ab_test
    ,e.ab_variant
    ,value.list[OFFSET(0)].element as eligible_type
		,e.visit_id
    ,a.beacon.browser_id
    ,a.beacon.user_id,
    case when beacon.event_source in ('web', 'customshops', 'craft_web')
          and beacon.is_mobile_device IS FALSE then 'desktop'
          when beacon.event_source in ('web', 'customshops', 'craft_web')
          and beacon.is_mobile_device IS TRUE then  'mobile_web'
          when beacon.event_source in ('ios','android')
          and REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'soe'
          when beacon.event_source in ('ios')
          and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe ios'
          when beacon.event_source in ('android')
          and not REGEXP_CONTAINS(lower(beacon.user_agent), lower('SellOnEtsy')) then 'boe android'
          else 'undefined' end AS app_platform_case,
    case when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.29')) then '6.29'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.28')) then '6.28'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.27')) then '6.27'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.26')) then '6.26'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.25')) then '6.25'
         when REGEXP_CONTAINS(lower(beacon.user_agent), lower('EtsyInc/6.24')) then '6.24'
          else 'other' end AS app_version,
    a.sequence_number
	from 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
  JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
      ON a.visit_id = e.visit_id
    JOIN UNNEST(beacon.ab.key_value)
	where 
		e.ab_test = config_flag
		AND e._date between start_date and end_date
    AND DATE(_PARTITIONTIME) between start_date and end_date
    AND beacon.event_name = 'getting_eligible_notifications_for_feed'
    AND key = type_flag
;

SELECT
v.ab_test,
v.ab_variant,
v.eligible,
u.eligible_type,
count(distinct v.visit_id) as visit_count,
count(distinct v.browser_id) as browser_count,
count(distinct v.user_id) as user_count
FROM exp_visits v
JOIN exp_visits_type u
  ON v.visit_id = u.visit_id and v.sequence_number = u.sequence_number
GROUP BY 1,2,3,4
;

end;


-- [iOS] Add Buy Now to “something in your cart is now on sale” Update
BEGIN 

-- *** ENTER BELOW *** --
DECLARE config_flag STRING default "deals_app.cart_listings_on_sale_with_buy_now_button"; -- enter experiment config flag
DECLARE start_date DATE default "2023-05-03"; -- enter experiment start date
DECLARE end_date DATE default "2023-05-15"; -- enter experiment end date

-- Bucketed visits for each variant during the experiment's timeframe
create temp table exp_visits
	as
	select 
		distinct 
    e.ab_test
    ,e.ab_variant
    ,value.list[OFFSET(0)].element as eligible
		,e.visit_id
    ,a.beacon.browser_id
    ,a.beacon.user_id
	from 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
  JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
      ON a.visit_id = e.visit_id
    JOIN UNNEST(beacon.ab.key_value)
	where 
		e.ab_test = config_flag
		AND e._date between start_date and end_date
    AND DATE(_PARTITIONTIME) between start_date and end_date
    AND beacon.event_name = 'getting_eligible_notifications_for_feed'
    AND key = config_flag
;

SELECT
ab_test,
ab_variant,
eligible,
count(distinct v.visit_id) as visit_count,
count(distinct case when notification_module = 'psfcl' then v.visit_id else null end) as visit_count_with_psfcl_in_feed,
count(distinct v.browser_id) as browser_count,
count(distinct case when notification_module = 'psfcl' then v.browser_id else null end) as browser_count_with_psfcl_in_feed,
count(distinct v.user_id) as user_count,
count(distinct case when notification_module = 'psfcl' then v.user_id else null end) as user_count_with_psfcl_in_feed
FROM exp_visits v
LEFT JOIN `etsy-data-warehouse-dev.nlao.update_notification_delivered` u
  ON v.visit_id = u.visit_id  
GROUP BY 1,2,3
;

end;







