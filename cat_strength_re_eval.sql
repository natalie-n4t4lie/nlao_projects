BEGIN
CREATE TEMP TABLE landing_p365 as (
select
	b._date,
	b.visit_id,
	b.run_date,
	b.user_id,
	b.visit_length,
	b.visit_duration,
	b.referrer_type,
	b.referring_url,
	b.referring_domain,
	b.referring_keyword,
	b.landing_event,
	b.exit_event,
	b.utm_campaign,
	b.region,
	b.landing_event_url,
	b.top_channel,
	b.second_channel,
	b.third_channel,
	b.platform,
	case when landing_event = "market" then lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "market/([^?]*)"), "_", " "), "\\%27", "")) else null end as landing_market_query,
	case when landing_event like "view%listing%" then safe_cast(regexp_substr(landing_event_url, "listing\\/(\\d*)") as int64) else null end as landing_listing_id,
	case when landing_event = "search" then regexp_replace(regexp_replace(regexp_substr(lower(landing_event_url), "q=([a-z0-9%+]+)"),"\\\\+"," "),"%20"," ") else null end as landing_search_query,
  case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(0)],"-","_") end as landing_cat_page_top_cat,
	case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(1)],"-","_") end as landing_cat_page_second_cat,
  case when landing_event = "category_page" then regexp_replace(split(regexp_substr(landing_event_url, "\\/c\\/([^\\?|\\&]+)"),"/")[safe_offset(2)],"-","_") end as landing_cat_page_third_cat
from
	`etsy-data-warehouse-prod.weblog.visits` b
where 
	b._date >= date_sub(current_date, interval 12 month)
	and b.platform in ("boe","desktop","mobile_web") --remove soe
	and b.is_admin_visit != 1 --remove admin
	and (b.user_id is null or user_id not in (
		select user_id from `etsy-data-warehouse-prod.rollups.seller_basics` where active_seller_status = 1)
		) --remove sellers
);

-- Landing event is a listing with “gift” in the title
-- Landing on market page where the query matches the is_gift definition outlined here
-- Landing on an Editor’s Picks page with a title containing “gift”
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.gifting_landing_p365` as (
SELECT DISTINCT
  v._date,
	v.visit_id,
	v.run_date,
	v.user_id,
	v.visit_length,
	v.visit_duration,
	v.referrer_type,
	v.referring_url,
	v.referring_domain,
	v.referring_keyword,
	v.landing_event,
	v.exit_event,
	v.utm_campaign,
	v.region,
	v.landing_event_url,
	v.top_channel,
	v.second_channel,
	v.third_channel,
	v.platform,
  v.landing_market_query,
  v.landing_listing_id,
	l.title,
  lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")) AS landing_feature_page,
	coalesce(v.landing_market_query,l.title,lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", ""))) AS keywords,
  1 AS is_gift
FROM landing_p365 v
LEFT JOIN `etsy-data-warehouse-prod.etsy_shard.listings` l
	ON v.landing_listing_id = l.listing_id
LEFT JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` q
	ON v.landing_market_query = q.query
WHERE 
regexp_contains(l.title, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') --Landing event is a listing with “gift” in the title
OR
q.is_gift = 1 -- Landing on market page where the query matches the is_gift definition outlined here
OR
regexp_contains(lower(regexp_replace(regexp_replace(regexp_substr(landing_event_url, "featured/([^?]*)"), "-", " "), "\\%27", "")), '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') -- Landing on an Editor’s Picks page with a title containing “gift”
)
;

-- H&L DEFINITION
-- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
-- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
-- Landing on a shop whose top category is H&L (can't classified subsub category on shop level, so not include in definition)
-- Landing on a H&L or relevant A&C category page

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.hl_landing_p365` as (
WITH distinct_taxo AS (
SELECT
taxonomy_id,
full_path,
split(full_path, '.')[safe_offset(0)] as top_cat,
split(full_path, '.')[safe_offset(1)] as second_cat,
split(full_path, '.')[safe_offset(2)] as third_cat,
case when split(full_path, '.')[safe_offset(0)] = "home_and_living" then "home"
	   when split(full_path, '.')[safe_offset(1)] in ("prints", "collectibles", "painting", "drawing_and_illustration")  then "home"
	else split(full_path, '.')[safe_offset(0)] end as vertical
FROM `etsy-data-warehouse-prod.structured_data.taxonomy`
),
hl_search AS (
SELECT
query,
taxonomy_id,
split(full_path, '.')[safe_offset(0)] as top_cat,
split(full_path, '.')[safe_offset(1)] as second_cat,
split(full_path, '.')[safe_offset(2)] as third_cat
FROM `etsy-data-warehouse-prod.search.query_sessions_new` q
JOIN distinct_taxo t
    ON q.classified_taxonomy_id = t.taxonomy_id
WHERE q._date >= date_sub(current_date, interval 12 month)
AND vertical = "home"
)
SELECT DISTINCT
  v._date,
	v.visit_id,
	v.run_date,
	v.user_id,
	v.visit_length,
	v.visit_duration,
	v.referrer_type,
	v.referring_url,
	v.referring_domain,
	v.referring_keyword,
	v.landing_event,
	v.exit_event,
	v.utm_campaign,
	v.region,
	v.landing_event_url,
	v.top_channel,
	v.second_channel,
	v.third_channel,
	v.platform,
  COALESCE(v.landing_cat_page_top_cat,t1.top_cat,t2.top_cat) AS top_cat,
  COALESCE(v.landing_cat_page_top_cat,t1.second_cat,t2.second_cat) AS second_cat,
  COALESCE(v.landing_cat_page_third_cat,t1.third_cat,t2.third_cat) AS third_cat,
  1 AS is_hl
FROM landing_p365 v
LEFT JOIN hl_search q
	ON v.landing_market_query = q.query
LEFT JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` a
  ON v.landing_listing_id = a.listing_id
LEFT JOIN distinct_taxo t1
  ON q.classified_taxonomy_id = t1.taxonomy_id
LEFT JOIN distinct_taxo t2
  ON a.taxonomy_id = t2.taxonomy_id
WHERE 
q.taxonomy_id IS NOT NULL  -- Landing on a market page with a query classified in the H&L or A&C subcategory taxonomies
OR a.taxonomy_id IN (SELECT taxonomy_id FROM distinct_taxo WHERE vertical = "home") -- Landing event is a listing in the home & listing category, or in key art & collectibles subcategories
OR v.landing_cat_page_top_cat = "home_and_living" OR v.landing_cat_page_second_cat IN ("prints", "collectibles", "painting", "drawing_and_illustration")--  Landing on a H&L or relevant A&C category page
)
;



END
;
