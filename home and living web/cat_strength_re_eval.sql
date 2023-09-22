-- create table with channel and market query mapping
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo` AS 
with slugs as (
select 
lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) as market_query,
case when channel_dimensions.tactic_high_level is null then "SEO"
  when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
  when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
  when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
  else 'Display/Social'
  end as channel,
visit_id,
total_gms,
converted
FROM `etsy-data-warehouse-prod.weblog.visits` v
LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(v.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(v.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when v.top_channel = 'social_promoted' then 'social_owned'
          when v.top_channel = 'social_organic' then 'social_earned'
          else v.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(v.second_channel) = lower(channel_dimensions.second_channel)
      and lower(v.third_channel) = lower(channel_dimensions.third_channel)
where _date >current_date -366
and landing_event='market'
AND
  ((channel_dimensions.tactic_high_level in 
  ('Display - Native', 'Email - Marketing', 'Owned Social - Other', 'PLA - Automatic', 'PLA - Comparison Shopping', 'PLA - Manual',
  'Paid Social - Curated', 'Push - Marketing', 'SEM - Brand', 'SEM - Non-Brand', 'SEM - Other', 'Video - Programmatic'))
  or v.top_channel = 'seo'
  )
)
, classifier as (
  SELECT
	q.query_raw,
	c.taxonomy_id,
	c.path AS query_item_type,
  c.top_cat AS query_top_cat,
  c.second_cat AS query_second_cat,
  c.third_cat AS query_third_cat,
	case when (regexp_contains(q.query_raw, "(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)")
		or regexp_contains(q.query_raw, "(\?i)\\boccasion|\\banniversary|\\bbirthday|\\bmothers day|\\bfathers day|\\bchristmas present")) then 1
	else 0 end as gift_query,
	row_number() over (partition by query_raw order by count(*) desc) as rn,
	count(*) as session_count,
from
	`etsy-data-warehouse-prod.search.query_sessions_new` q
join 
	`etsy-data-warehouse-dev.nlao.taxonomy_parsed` c on q.classified_taxonomy_id = c.taxonomy_id
where 
	q._date >= current_date - 365 -- get a full year of classifications to get maximal coverage
and q.query_raw in (
	select market_query from slugs
	)
group by 
	1,2,3,4,5,6
),
most_common AS (
select 
	* 
from 
	classifier
where rn = 1	-- most common categorization for a given query
)
select 
market_query,
channel,
taxonomy_id,
query_top_cat,
query_second_cat,
query_third_cat,
query_item_type,
gift_query,
visit_id,
total_gms,
converted
from
most_common
join 
slugs 
on query_raw=slugs.market_query
where query_top_cat='home_and_living' or query_second_cat IN ("prints", "collectibles", "painting", "drawing_and_illustration") 
;

select
sum(past_year_gms)
from `etsy-data-warehouse-prod.listing_mart.listing_gms`
;--11935554618.62

SELECT
count(visit_id)
FROM `etsy-data-warehouse-prod.weblog.visits`
where _date >= date_sub(current_date, interval 12 month)
;--13280159133

-- 
SELECT
query_top_cat,
query_second_cat,
query_third_cat,
query_item_type,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE query_item_type != query_top_cat AND query_item_type != query_second_cat
GROUP BY 1,2,3,4
ORDER BY 5 DESC
;

SELECT
query_top_cat,
query_second_cat,
query_third_cat,
query_item_type,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE channel = 'SEO'
AND (query_item_type != query_top_cat AND query_item_type != query_second_cat)
GROUP BY 1,2,3,4
ORDER BY 5 DESC
;

SELECT
query_top_cat,
query_second_cat,
query_third_cat,
query_item_type,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE channel = 'SEM'
AND (query_item_type != query_top_cat AND query_item_type != query_second_cat)
GROUP BY 1,2,3,4
ORDER BY 5 DESC
;

---- second and third level
SELECT
query_second_cat,
query_third_cat,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE query_second_cat IS NOT NULL AND query_third_cat IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
;

SELECT
query_second_cat,
query_third_cat,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE channel = 'SEO'
AND query_second_cat IS NOT NULL AND query_third_cat IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
;

SELECT
query_second_cat,
query_third_cat,
COUNT(visit_id) AS visit,
SUM(total_gms) AS gms
FROM `etsy-data-warehouse-dev.nlao.hl_market_landing_taxo`
WHERE channel = 'SEM'
AND query_second_cat IS NOT NULL AND query_third_cat IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
;


-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.landing_p365` as (
-- select
-- 	DISTINCT
-- 	b._date,
-- 	b.visit_id,
-- 	b.user_id,
-- 	b.landing_event,
-- 	b.region,
-- 	b.landing_event_url,
-- 	case when landing_event = "market" then lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) else null end as landing_market_query,
-- 	case when landing_event like "view%listing%" then safe_cast(regexp_substr(landing_event_url, "listing\\/(\\d*)") as int64) else null end as landing_listing_id,
-- 	case when landing_event = "search" then regexp_replace(regexp_replace(regexp_substr(lower(landing_event_url), "q=([a-z0-9%+]+)"),"\\\\+"," "),"%20"," ") else null end as landing_search_query,
--   case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(0)],"-","_") end as landing_cat_page_top_cat,
-- 	case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(1)],"-","_") end as landing_cat_page_second_cat,
--   case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(2)],"-","_") end as landing_cat_page_third_cat,
-- 	case when channel_dimensions.tactic_high_level is null then "SEO"
--   when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
--   when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
--   when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
--   else 'Display/Social'
--   end as channel,
-- from
-- 	`etsy-data-warehouse-prod.weblog.visits` b
-- LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
-- 	ON lower(b.utm_campaign) = lower(channel_dimensions.utm_campaign)
--       and lower(b.utm_medium) = lower(channel_dimensions.utm_medium)
--       and lower((case when b.top_channel = 'social_promoted' then 'social_owned'
--           when b.top_channel = 'social_organic' then 'social_earned'
--           else b.top_channel end)) = lower(channel_dimensions.top_channel)
--       and lower(b.second_channel) = lower(channel_dimensions.second_channel)
--       and lower(b.third_channel) = lower(channel_dimensions.third_channel)
-- where 
-- 	b._date >= date_sub(current_date, interval 12 month)
-- 	and b.platform in ("boe","desktop","mobile_web") --remove soe
-- 	and b.is_admin_visit != 1 --remove admin
-- 	and (b.user_id is null or user_id not in (
-- 		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
-- 		) --remove sellers
-- 	AND (landing_event IN ("search","category_page","market") OR landing_event LIKE "view%listing%")
-- );

-- -- Landing event is a listing with “gift” in the title
-- -- Landing on market page where the query matches the is_gift definition outlined here
-- -- Landing on an Editor’s Picks page with a title containing “gift”
-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.gifting_landing_p365` as (
-- SELECT DISTINCT
--   v._date,
-- 	v.visit_id,
-- 	v.user_id,
-- 	v.landing_event,
-- 	v.region,
-- 	v.landing_event_url,
-- 	v.channel,
--   v.landing_market_query,
--   v.landing_listing_id,
-- 	l.title,
--   lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")) AS landing_feature_page,
-- 	coalesce(v.landing_market_query,l.title,lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", ""))) AS keywords,
--   1 AS is_gift
-- FROM `etsy-data-warehouse-dev.nlao.landing_p365` v
-- LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.listings` l
-- 	ON v.landing_listing_id = l.listing_id
-- LEFT JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` q
-- 	ON v.landing_market_query = q.query
-- WHERE 
-- regexp_contains(l.title, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') --Landing event is a listing with “gift” in the title
-- OR q.is_gift = 1 -- Landing on market page where the query matches the is_gift definition outlined here
-- OR regexp_contains(lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")), '(\?i)\\bgift') -- Landing on an Editor’s Picks page with a title containing “gift”
-- )
-- ;

-- H&L DEFINITION
-- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
-- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
-- Landing on a shop whose top category is H&L (can't classified subsub category on shop level, so not include in definition)
-- Landing on a H&L or relevant A&C category page

-- CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.hl_landing_p365` as (
-- WITH
-- distinct_taxo as (
-- 	select
-- 	distinct 
-- 	taxonomy_id,
-- 	top_level_cat_new,
-- 	second_level_cat_new,
-- 	third_level_cat_new,
-- 	case when top_level_cat_new = "home_and_living" then 1
-- 	when second_level_cat_new in ("prints", "collectibles", "painting", "drawing_and_illustration")  then 1
-- 	else 0 end as is_home
-- 	from `etsy-data-warehouse-prod.materialized.listing_categories_taxonomy`
-- )
-- , query_label as (
-- 	select
-- 		s.query,
-- 		t.top_level_cat_new,
-- 		t.second_level_cat_new,
-- 		t.third_level_cat_new,
-- 		t.is_home
-- 	from `etsy-data-warehouse-prod.search.query_sessions_new` s
-- 	join distinct_taxo t 
-- 	on s.classified_taxonomy_id = t.taxonomy_id
-- 	where _date >= date_sub(current_date, interval 12 month)
-- )
-- SELECT 
-- 	v._date,
-- 	v.visit_id,
-- 	v.user_id,
-- 	v.landing_event,
-- 	v.region,
-- 	v.landing_event_url,
-- 	v.channel,
--   COALESCE(v.landing_cat_page_top_cat,t1.top_level_cat_new,q.top_level_cat_new) AS top_cat,
--   COALESCE(v.landing_cat_page_top_cat,t1.second_level_cat_new,q.second_level_cat_new) AS second_cat,
--   COALESCE(v.landing_cat_page_third_cat,t1.third_level_cat_new,q.third_level_cat_new) AS third_cat,
--   1 AS is_hl
-- FROM `etsy-data-warehouse-dev.nlao.landing_p365` v
-- LEFT JOIN query_label q
-- 	ON v.landing_market_query = q.query
-- LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` a
--   ON v.landing_listing_id = a.listing_id
-- LEFT JOIN distinct_taxo t1
--   ON a.taxonomy_id = t1.taxonomy_id
-- WHERE 
-- q.is_home = 1 -- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
-- OR t1.is_home = 1 -- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
-- OR v.landing_cat_page_top_cat = "home_and_living" OR v.landing_cat_page_second_cat IN ("prints", "collectibles", "painting", "drawing_and_illustration")--  Landing on a H&L or relevant A&C category page
-- )
-- ;

--------------------------------------analysis-------------------------------------------------------------------------
-- ended up using `etsy-data-warehouse-dev.awaagner.hl_gift_visits_classified` since it has most info ready
create or replace table `etsy-data-warehouse-dev.nlao.market_data` as (
with market as (
	select
	v._date,
	v.visit_id,
	v.converted,
	v.bounced,
	v.orders,
	v.total_gms,
	t.transaction_id,
	t.listing_id,
	t.trans_gms_net,
  case when channel_dimensions.tactic_high_level is null then "SEO"
  when channel_dimensions.tactic_high_level in ('Email - Marketing','Push - Marketing') then 'CRM'
  when channel_dimensions.tactic_high_level like 'SEM%' then 'SEM'
  when channel_dimensions.tactic_high_level like 'PLA%' then 'PLA'
  else 'Display/Social'
  end as channel,
	case when tp.top_cat = "home_and_living" then "home"
	     when tp.second_cat in ("prints", "collectibles", "painting", "drawing_and_illustration")  then "home"
	     else tp.top_cat end as purch_vertical,
	tp.top_cat as purch_cat,
	tp.second_cat as purch_second_cat,
	tp.third_cat as purch_third_cat,
	lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) as query
FROM `etsy-data-warehouse-prod.weblog.visits` v
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
left join `etsy-data-warehouse-dev.nlao.taxonomy_parsed` tp
on t.taxonomy_id = tp.taxonomy_id
where v._date >= date_sub(current_date, interval 12 month)
and landing_event = "market"
and ((converted = 1 and transaction_id is not NULL)
or converted = 0)
AND
  ((channel_dimensions.tactic_high_level in 
  ('Display - Native', 'Email - Marketing', 'Owned Social - Other', 'PLA - Automatic', 'PLA - Comparison Shopping', 'PLA - Manual',
  'Paid Social - Curated', 'Push - Marketing', 'SEM - Brand', 'SEM - Non-Brand', 'SEM - Other', 'Video - Programmatic'))
  or v.top_channel = 'seo'
  )
)
, distinct_taxo as (
	select
	distinct 
	taxonomy_id,
	top_cat,
	second_cat,
	third_cat,
  path as item,
	case when top_cat = "home_and_living" then "home"
	when second_cat in ("prints", "collectibles", "painting", "drawing_and_illustration")  then "home"
	else top_cat end as vertical
	from `etsy-data-warehouse-dev.nlao.taxonomy_parsed`
)
, query_label as (
	select
		s.query_raw,
		t.top_cat,
		t.second_cat,
		t.third_cat,
    t.item,
		t.vertical,
		count(*) as n_queries
	from `etsy-data-warehouse-prod.search.query_sessions_new` s
	join distinct_taxo t 
	on s.classified_taxonomy_id = t.taxonomy_id
	where _date >= date_sub(current_date, interval 12 month)
	group by 1,2,3,4,5,6
	qualify row_number() over(partition by query_raw order by count(*) desc) = 1
)
	select
		visit_id,
		query as market_page_query,
		q.top_cat as market_top_cat,
		q.second_cat as market_second_cat,
		q.third_cat as market_third_cat,
    q.item as market_item,
		q.vertical as market_vertical,
    m.channel,
		-- CR
		max(converted) as has_conversion,
		max(case when m.purch_vertical = q.vertical then 1 else 0 end) as conversion_in_vertical,
		max(case when m.purch_vertical = q.vertical and m.purch_second_cat = q.second_cat then 1 else 0 end) as conversion_in_subcat,
		max(case when m.purch_vertical != q.vertical then 1 else 0 end) as conversion_in_other_vertical,
		-- GMS
		sum(trans_gms_net) as total_gms,
		sum(case when m.purch_vertical = q.vertical then trans_gms_net else 0 end) as gms_in_vertical,
		sum(case when m.purch_vertical = q.vertical and m.purch_second_cat = q.second_cat then trans_gms_net else 0 end) as gms_in_subcat,
		sum(case when m.purch_vertical != q.vertical then trans_gms_net else 0 end) as gms_in_other_vertical

	from market m
	join query_label q
	on m.query = q.query_raw
	group by 1,2,3,4,5,6,7,8
);


select
channel,
count(visit_id),
sum(total_gms)
from `etsy-data-warehouse-dev.nlao.market_data` 
WHERE market_vertical = "home" 
group by 1
;

-- OVERALL
select
  market_top_cat,
  market_second_cat,
  market_third_cat,
	market_item,
  count(visit_id) AS landings,
	SUM(total_gms) AS gms
from `etsy-data-warehouse-dev.nlao.market_data` 
where market_vertical = "home" 
	AND market_item != market_top_cat 
	AND market_item != market_second_cat 
group by 1,2,3,4
order by 5 desc
LIMIT 100
;

-- SEO
select
  market_top_cat,
  market_second_cat,
  market_third_cat,
	market_item,
  count(visit_id) AS landings,
	SUM(total_gms) AS gms
from `etsy-data-warehouse-dev.nlao.market_data` 
where market_vertical = "home" 
	AND market_item != market_top_cat 
	AND market_item != market_second_cat 
	AND channel = 'SEO'
group by 1,2,3,4
order by 5 desc
LIMIT 100
;

-- SEM
select
  market_top_cat,
  market_second_cat,
  market_third_cat,
	market_item,
  count(visit_id) AS landings,
	SUM(total_gms) AS gms
from `etsy-data-warehouse-dev.nlao.market_data` 
where market_vertical = "home" 
	AND market_item != market_top_cat 
	AND market_item != market_second_cat 
	AND channel = 'SEM'
group by 1,2,3,4
order by 5 desc
LIMIT 100
;

--------------------------------------------------- GIFTING------------------------------------------------------------
-- ended up using `etsy-data-warehouse-dev.awaagner.hl_gift_visits_classified` since it has most info ready
-- OVERALL
SELECT
query,
COUNT(*) AS visits,
sum(total_gms) AS gms
FROM `etsy-data-warehouse-dev.awaagner.hl_gift_visits_classified` g
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON g.visit_id = q.visit_id
WHERE q._date >= date_sub(current_date, interval 12 month) 
  AND gift_visit = 1
	AND query != "gift" 
	AND length(query) > 2
	AND g.landing_event = "market"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
;

-- SEO
SELECT
query,
COUNT(*) AS visits,
sum(total_gms) AS gms
FROM `etsy-data-warehouse-dev.awaagner.hl_gift_visits_classified` b
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON b.visit_id = q.visit_id
LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(b.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(b.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when b.top_channel = 'social_promoted' then 'social_owned'
          when b.top_channel = 'social_organic' then 'social_earned'
          else b.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(b.second_channel) = lower(channel_dimensions.second_channel)
      and lower(b.third_channel) = lower(channel_dimensions.third_channel)
WHERE 
  q._date >= date_sub(current_date, interval 12 month) 
  AND channel_dimensions.tactic_high_level is null
  AND b.gift_visit = 1
	AND query != "gift"
	AND length(query) > 2
	AND landing_event = "market"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
;

-- SEM 
SELECT
query,
COUNT(*) AS visits,
sum(total_gms) AS gms
FROM `etsy-data-warehouse-dev.awaagner.hl_gift_visits_classified` b
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
  ON b.visit_id = q.visit_id
LEFT JOIN `etsy-data-warehouse-prod.buyatt_mart.channel_dimensions` AS channel_dimensions 
	ON lower(b.utm_campaign) = lower(channel_dimensions.utm_campaign)
      and lower(b.utm_medium) = lower(channel_dimensions.utm_medium)
      and lower((case when b.top_channel = 'social_promoted' then 'social_owned'
          when b.top_channel = 'social_organic' then 'social_earned'
          else b.top_channel end)) = lower(channel_dimensions.top_channel)
      and lower(b.second_channel) = lower(channel_dimensions.second_channel)
      and lower(b.third_channel) = lower(channel_dimensions.third_channel)
WHERE 
  q._date >= date_sub(current_date, interval 12 month) 
  AND channel_dimensions.tactic_high_level like 'SEM%'
  AND b.gift_visit = 1
	AND query != "gift"
	AND length(query) > 2
	AND landing_event = "market"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
;






