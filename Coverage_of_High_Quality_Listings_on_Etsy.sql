CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.landing_p365` as (
select
	DISTINCT
	b._date,
	b.visit_id,
	b.user_id,
	b.landing_event,
	b.region,
	b.landing_event_url,
	case when landing_event = "market" then lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) else null end as landing_market_query,
	case when landing_event like "view%listing%" then safe_cast(regexp_substr(landing_event_url, "listing\\/(\\d*)") as int64) else null end as landing_listing_id,
	case when landing_event = "search" then regexp_replace(regexp_replace(regexp_substr(lower(landing_event_url), "q=([a-z0-9%+]+)"),"\\\\+"," "),"%20"," ") else null end as landing_search_query,
  case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(0)],"-","_") end as landing_cat_page_top_cat,
	case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(1)],"-","_") end as landing_cat_page_second_cat,
  case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(2)],"-","_") end as landing_cat_page_third_cat,
	case when channel_dimensions.tactic_high_level is null then "SEO"
  when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
  when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
  when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
  else 'Display/Social'
  end as channel,
from
	`etsy-data-warehouse-prod.weblog.visits` b
LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(b.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(b.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when b.top_channel = 'social_promoted' then 'social_owned'
          when b.top_channel = 'social_organic' then 'social_earned'
          else b.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(b.second_channel) = lower(channel_dimensions.second_channel)
      and lower(b.third_channel) = lower(channel_dimensions.third_channel)
where 
	b._date >= date_sub(current_date, interval 12 month)
	and b.platform in ("boe","desktop","mobile_web") --remove soe
	and b.is_admin_visit != 1 --remove admin
	and (b.user_id is null or user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		) --remove sellers
	AND (landing_event IN ("search","category_page","market") OR landing_event LIKE "view%listing%")
);

-- Landing event is a listing with “gift” in the title
-- Landing on market page where the query matches the is_gift definition outlined here
-- Landing on an Editor’s Picks page with a title containing “gift”
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.gifting_landing_p365` as (
SELECT DISTINCT
  v._date,
	v.visit_id,
	v.user_id,
	v.landing_event,
	v.region,
	v.landing_event_url,
	channel,
  v.landing_market_query,
  v.landing_listing_id,
	l.title,
  lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")) AS landing_feature_page,
	coalesce(v.landing_market_query,l.title,lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", ""))) AS keywords,
  1 AS is_gift
FROM `etsy-data-warehouse-dev.nlao.landing_p365` v
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.listings` l
	ON v.landing_listing_id = l.listing_id
LEFT JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` q
	ON v.landing_market_query = q.query
WHERE 
regexp_contains(l.title, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') --Landing event is a listing with “gift” in the title
OR q.is_gift = 1 -- Landing on market page where the query matches the is_gift definition outlined here
OR regexp_contains(lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")), '(\?i)\\bgift') -- Landing on an Editor’s Picks page with a title containing “gift”
)
;

-- H&L DEFINITION
-- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
-- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
-- Landing on a shop whose top category is H&L (can't classified subsub category on shop level, so not include in definition)
-- Landing on a H&L or relevant A&C category page

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.hl_landing_p365` as (
WITH
distinct_taxo as (
	select
	distinct 
	taxonomy_id,
	top_level_cat_new,
	second_level_cat_new,
	third_level_cat_new,
	case when top_level_cat_new = "home_and_living" then 1
	when second_level_cat_new in ("prints", "collectibles", "painting", "drawing_and_illustration")  then 1
	else 0 end as is_home
	from `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy`
)
, query_label as (
	select
		s.query,
		t.top_level_cat_new,
		t.second_level_cat_new,
		t.third_level_cat_new,
		t.is_home
	from `etsy-data-warehouse-prod.search.query_sessions_new` s
	join distinct_taxo t 
	on s.classified_taxonomy_id = t.taxonomy_id
	where _date >= date_sub(current_date, interval 12 month)
)
SELECT 
	v._date,
	v.visit_id,
	v.user_id,
	v.landing_event,
	v.region,
	v.landing_event_url,
	v.channel,
  COALESCE(v.landing_cat_page_top_cat,t1.top_level_cat_new,q.top_level_cat_new) AS top_cat,
  COALESCE(v.landing_cat_page_top_cat,t1.second_level_cat_new,q.second_level_cat_new) AS second_cat,
  COALESCE(v.landing_cat_page_third_cat,t1.third_level_cat_new,q.third_level_cat_new) AS third_cat,
  1 AS is_hl
FROM `etsy-data-warehouse-dev.nlao.landing_p365` v
LEFT JOIN query_label q
	ON v.landing_market_query = q.query
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` a
  ON v.landing_listing_id = a.listing_id
LEFT JOIN distinct_taxo t1
  ON a.taxonomy_id = t1.taxonomy_id
WHERE 
q.is_home = 1 -- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
OR t1.is_home = 1 -- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
OR v.landing_cat_page_top_cat = "home_and_living" OR v.landing_cat_page_second_cat IN ("prints", "collectibles", "painting", "drawing_and_illustration")--  Landing on a H&L or relevant A&C category page
)
;

SELECT
top_cat,
second_cat,
third_cat,
count(*) AS visits
FROM `etsy-data-warehouse-dev.nlao.hl_landing_p365` g
GROUP BY 1,2,3
ORDER BY 4 DESC
;

-- OVERALL
SELECT
query,
COUNT(*) AS visits
FROM `etsy-data-warehouse-dev.nlao.gifting_landing_p365` g
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON g.visit_id = q.visit_id
WHERE q._date >= date_sub(current_date, interval 12 month) 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10000
;

-- SEO
SELECT
query,
COUNT(*) AS visits,
FROM `etsy-data-warehouse-dev.nlao.gifting_landing_p365` g
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON g.visit_id = q.visit_id
WHERE 
  q._date >= date_sub(current_date, interval 12 month) 
  AND channel = "SEO"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10000
;

-- SEM 
SELECT
  query,
  COUNT(*) AS visits
FROM `etsy-data-warehouse-dev.nlao.gifting_landing_p365` g
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON g.visit_id = q.visit_id
WHERE
  q._date >= date_sub(current_date, interval 12 month)
  AND channel = "SEM"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10000
;
