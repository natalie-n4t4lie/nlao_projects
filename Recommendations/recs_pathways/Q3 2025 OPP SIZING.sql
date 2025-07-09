-- BOE RECS LANDING COVERAGE
WITH page_visit AS (
SELECT
visit_id,
MAX(CASE WHEN event_type IN ('view_listing') THEN 1 ELSE 0 END) AS listing_visit,
MAX(CASE WHEN event_type IN (
"boe_listing_screen_similar_listings_organic_only",
"view_favorites_recommendations",
"homescreen_recent_favorites",
"homescreen_recently_viewed_horiz",
"sdl_category_page",
"space_page",
"gift_mode_gift_idea",
"boe_sdl_landing_page",
"boe_landing_page_listings",
"programmable_hub_page"
)
THEN 1 ELSE 0 END) AS recs_landing_visit
FROM `etsy-data-warehouse-prod.weblog.events`
GROUP BY 1
)
SELECT
CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
AND is_mobile_device = 0 THEN 'desktop'
when event_source IN ('web', 'customshops', 'craft_web')
AND is_mobile_device = 1 THEN 'mobile_web'
when event_source IN ('ios','android')
AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
when event_source IN ('ios')
AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
when event_source IN ('android')
AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
else 'undefined' END AS app_platform_case,
SUM(a.total_gms) AS total_gms,
SUM(CASE WHEN recs_landing_visit = 1 THEN a.total_gms ELSE NULL END) AS recs_landing_visit_gms,
SUM(CASE WHEN listing_visit = 1 THEN a.total_gms ELSE NULL END) AS listing_visit_gms,
COUNT(DISTINCT a.visit_id) AS total_visit,
COUNT(DISTINCT CASE WHEN recs_landing_visit = 1 THEN a.visit_id ELSE NULL END) AS recs_landing_visit,
SUM(CASE WHEN listing_visit = 1 THEN a.total_gms ELSE NULL END) AS listing_visit,
from
`etsy-data-warehouse-prod.weblog.recent_visits` a
left join page_visit p
ON a.visit_id = p.visit_id
where
a._date >= current_date - 30
group by ALL
;

-- GROUP LANDINGS
SELECT
event_source,
COUNT(DISTINCT CASE WHEN event_type = "boe_listing_screen_similar_listings_organic_only" THEN e.visit_id ELSE NULL END) AS boe_listing_screen_similar_listings_organic_only,
COUNT(DISTINCT CASE WHEN event_type = "view_favorites_recommendations" THEN e.visit_id ELSE NULL END) AS view_favorites_recommendations,
COUNT(DISTINCT CASE WHEN event_type = "homescreen_recent_favorites" THEN e.visit_id ELSE NULL END) AS homescreen_recent_favorites,
COUNT(DISTINCT CASE WHEN event_type = "homescreen_recently_viewed_horiz" THEN e.visit_id ELSE NULL END) AS homescreen_recently_viewed_horiz,
COUNT(DISTINCT CASE WHEN event_type = "sdl_category_page" THEN e.visit_id ELSE NULL END) AS sdl_category_page,
COUNT(DISTINCT CASE WHEN event_type = "space_page" THEN e.visit_id ELSE NULL END) AS space_page,
COUNT(DISTINCT CASE WHEN event_type = "gift_mode_gift_idea" THEN e.visit_id ELSE NULL END) AS gift_mode_gift_idea,
COUNT(DISTINCT CASE WHEN event_type = "programmable_hub_page" THEN e.visit_id ELSE NULL END) AS programmable_hub_page,
COUNT(CASE WHEN event_type = "boe_listing_screen_similar_listings_organic_only" THEN e.visit_id ELSE NULL END) AS boe_listing_screen_similar_listings_organic_only,
COUNT(CASE WHEN event_type = "view_favorites_recommendations" THEN e.visit_id ELSE NULL END) AS view_favorites_recommendations,
COUNT(CASE WHEN event_type = "homescreen_recent_favorites" THEN e.visit_id ELSE NULL END) AS homescreen_recent_favorites,
COUNT(CASE WHEN event_type = "homescreen_recently_viewed_horiz" THEN e.visit_id ELSE NULL END) AS homescreen_recently_viewed_horiz,
COUNT(CASE WHEN event_type = "sdl_category_page" THEN e.visit_id ELSE NULL END) AS sdl_category_page,
COUNT(CASE WHEN event_type = "space_page" THEN e.visit_id ELSE NULL END) AS space_page,
COUNT(CASE WHEN event_type = "gift_mode_gift_idea" THEN e.visit_id ELSE NULL END) AS gift_mode_gift_idea,
COUNT(CASE WHEN event_type = "programmable_hub_page" THEN e.visit_id ELSE NULL END) AS programmable_hub_page
FROM `etsy-data-warehouse-prod.weblog.events` e
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
  ON e.visit_id = v.visit_id
WHERE e._date >= CURRENT_DATE - 7
AND v._date >= CURRENT_DATE - 7
AND is_mobile_device = 1 
AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy'))
GROUP BY ALL
;

-- SDL landing: Boe_sdl_landing_page by referer (ref) AND spec 
SELECT
 CASE WHEN beacon.event_source IN ('web', 'customshops', 'craft_web')
          AND beacon.is_mobile_device IS FALSE THEN 'desktop'
          when beacon.event_source IN ('web', 'customshops', 'craft_web')
          AND beacon.is_mobile_device IS TRUE THEN  'mobile_web'
          when beacon.event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when beacon.event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when beacon.event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          else 'undefined' END AS app_platform_case,
  a.beacon.ref,
  (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "spec") AS spec,
  COUNT(DISTINCT visit_id) AS visit_distinct_ct,
  COUNT(visit_id) AS page_ct,
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
  JOIN UNNEST(beacon.ab.key_value)
WHERE a.beacon.event_name = "boe_sdl_landing_page"
AND DATE(_PARTITIONTIME) >= CURRENT_DATE - 7
GROUP BY ALL
;

-- Generic landing: Boe_landing_page_listings by spec
SELECT
  CASE WHEN beacon.event_source IN ('web', 'customshops', 'craft_web')
          AND beacon.is_mobile_device IS FALSE THEN 'desktop'
          when beacon.event_source IN ('web', 'customshops', 'craft_web')
          AND beacon.is_mobile_device IS TRUE THEN  'mobile_web'
          when beacon.event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when beacon.event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when beacon.event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(beacon.user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE 'undefined' END AS app_platform_case,         
  (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = "spec") AS spec,
  COUNT(DISTINCT visit_id) AS visit_distinct_ct,
  COUNT(visit_id) AS page_ct
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
  JOIN UNNEST(beacon.ab.key_value)
WHERE a.beacon.event_name = "boe_landing_page_listings"
AND DATE(_PARTITIONTIME) >= CURRENT_DATE - 7
GROUP BY ALL
;

-- DENOMINATOR FOR % VISITS 
SELECT
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device =1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          else 'undefined' END AS app_platform_case,
COUNT(DISTINCT a.visit_id) AS visit_ct
FROM `etsy-data-warehouse-prod.weblog.recent_visits` a
JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON a.visit_id = e.visit_id
WHERE a._date >= current_date - 7
AND event_type IN ("boe_listing_screen_similar_listings_organic_only","view_favorites_recommendations", "homescreen_recent_favorites" , "homescreen_recently_viewed_horiz","sdl_category_page", "space_page", "gift_mode_gift_idea","boe_listing_screen_similar_listings_organic_only" , "view_favorites_recommendations", "homescreen_recent_favorites","homescreen_recently_viewed_horiz", "sdl_category_page", "space_page" ,"gift_mode_gift_idea","boe_landing_page_listings","boe_sdl_landing_page","programmable_hub_page")
GROUP BY ALL
;
