-- Sizing for SEM/SEO traffic to cat and broad market pages

-- PAST YEAR VISIT COUNT BY PLATFORM (FILTERING OUT SELLER AND ADMIN VISITS)
SELECT
  case when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 0 then 'desktop'
          when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 1 then  'mobile_web'
          when event_source in ('ios','android')
          and REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'soe'
          when event_source in ('ios')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe ios'
          when event_source in ('android')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe android'
          else 'undefined' end AS platform,
  count(v.visit_id) AS visit_count
FROM `etsy-data-warehouse-prod.weblog.visits` v
WHERE 
  v._date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR) AND CURRENT_DATE
  and v.is_admin_visit != 1 --remove admin
	and (v.user_id is null or user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		)  
GROUP BY 1
;

-- CREATE TABLE `etsy-data-warehouse-dev.nlao.hl_category_market_query` FOR CATEGORY PAGE URL MATCHING from a sheet: https://docs.google.com/spreadsheets/d/1pX7iopiamp60p7pUJHNd18ojOoXOw_UZXD5CiDAlNj4/edit#gid=0


-- CREATE A TABLE THAT COLLECTS ALL CATEGORY PAGE VISIT IN THE PAST YEAR AND EXTRACT THE CATEGORY PAGE PATH FROM URL

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.cat_page_visit_p1y AS (
    SELECT
    	v._date,
      v.visit_id,
      v.converted,
      v.bounced,
      v.orders,
      t.transaction_id,
      t.listing_id,
      t.trans_gms_net,
      case when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 0 then 'desktop'
          when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 1 then  'mobile_web'
          when event_source in ('ios','android')
          and REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'soe'
          when event_source in ('ios')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe ios'
          when event_source in ('android')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe android'
          else 'undefined' end AS app_platform,
      case when channel_dimensions.tactic_high_level is null then "SEO"
            when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
            when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
            when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
            else 'Display/Social'
            end as channel,
      url,
      replace(replace(LOWER(REGEXP_SUBSTR(e.url, "/c/([^?|&|%]*)",1,1)),"/","."),"-","_") AS cat,
  FROM `etsy-data-warehouse-prod.weblog.visits` v
  JOIN `etsy-data-warehouse-prod.weblog.events` e 
    ON v.visit_id = e.visit_id
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(v.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(v.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when v.top_channel = 'social_promoted' then 'social_owned'
          when v.top_channel = 'social_organic' then 'social_earned'
          else v.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(v.second_channel) = lower(channel_dimensions.second_channel)
      and lower(v.third_channel) = lower(channel_dimensions.third_channel)
  left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` t
    on v.visit_id = t.visit_id
    and v._date = t._date
  WHERE v._DATE BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH) AND CURRENT_DATE
      AND e._DATE BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH) AND CURRENT_DATE
      AND is_admin_visit !=1 --remove admin
	    AND (v.user_id is null or v.user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		) --remove sellers
    AND event_type = "category_page"
    and ((converted = 1 and transaction_id is not NULL) or converted = 0)
)
;


--HOW MUCH TRAFFIC DOES EACH H&L CATEGORY PAGE HAS?
SELECT
  c.app_platform,
  c.channel,
  c.cat,
  count(distinct c.visit_id) AS visit_count
FROM `etsy-data-warehouse-dev`.nlao.cat_page_visit_p1y c
JOIN `etsy-data-warehouse-dev.nlao.hl_category_market_query` h
  ON c.cat = h.full_url_path
  GROUP BY 1,2,3
;

SELECT
  app_platform,
  count(distinct c.visit_id) AS visit_count
FROM `etsy-data-warehouse-dev`.nlao.cat_page_visit_p1y c
JOIN `etsy-data-warehouse-dev.nlao.hl_category_market_query` h
  ON c.cat = h.full_url_path
  GROUP BY 1
;

-- create market page query from the past 1 year
-- get market page visits in the past 1 year and its market query
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.market_page_visit_p1y AS (
    SELECT
    	v._date,
      v.visit_id,
      v.converted,
      v.bounced,
      v.orders,
      t.transaction_id,
      t.listing_id,
      t.trans_gms_net,
      case when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 0 then 'desktop'
          when event_source in ('web', 'customshops', 'craft_web')
          and is_mobile_device = 1 then  'mobile_web'
          when event_source in ('ios','android')
          and REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'soe'
          when event_source in ('ios')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe ios'
          when event_source in ('android')
          and not REGEXP_CONTAINS(lower(user_agent), lower('SellOnEtsy')) then 'boe android'
          else 'undefined' end AS app_platform,
      case when channel_dimensions.tactic_high_level is null then "SEO"
            when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
            when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
            when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
            else 'Display/Social'
            end as channel,
      url,
      lower(regexp_replace(regexp_replace(regexp_substr(url, "market/([^?]*)"), "_", " "), "\\%27", "")) as query
  FROM `etsy-data-warehouse-prod.weblog.visits` v
  JOIN `etsy-data-warehouse-prod.weblog.events` e 
    ON v.visit_id = e.visit_id
  LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(v.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(v.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when v.top_channel = 'social_promoted' then 'social_owned'
          when v.top_channel = 'social_organic' then 'social_earned'
          else v.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(v.second_channel) = lower(channel_dimensions.second_channel)
      and lower(v.third_channel) = lower(channel_dimensions.third_channel)
  left join `etsy-data-warehouse-prod.visit_mart.visits_transactions` t
    on v.visit_id = t.visit_id
    and v._date = t._date
  WHERE v._DATE BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH) AND CURRENT_DATE
      AND e._DATE BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH) AND CURRENT_DATE
      AND is_admin_visit !=1 --remove admin
	    AND (v.user_id is null or v.user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		) --remove sellers
    AND event_type = "market"
    and ((converted = 1 and transaction_id is not NULL) or converted = 0)
)
;


-- redirected traffic market queries and its current conversion rate and gms
with cte AS (
	select
		visit_id,
		full_path,
    category,
    app_platform,
    channel,
		-- CR
		max(converted) as has_conversion,
		-- GMS
		sum(trans_gms_net) as total_gms,
	FROM `etsy-data-warehouse-dev`.nlao.market_page_visit_p1y m
  JOIN `etsy-data-warehouse-dev.nlao.hl_category_market_query` n
  ON m.query = n.category_keyword
	group by 1,2,3,4,5
)
select
		full_path,
    category,
    app_platform,
    count(distinct visit_id) as landings,
    sum(has_conversion)/count(distinct visit_id) as overall_cr,
    sum(total_gms)/count(distinct visit_id) as overall_gms_per_visit
from cte
where channel in ('SEO','SEM')
group by 1,2,3
;



