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
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "boe_listing_screen_similar_listings_organic_only" THEN a.visit_id ELSE NULL END) AS boe_listing_screen_similar_listings_organic_only,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "view_favorites_recommendations" THEN a.visit_id ELSE NULL END) AS view_favorites_recommendations,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "homescreen_recent_favorites" THEN a.visit_id ELSE NULL END) AS homescreen_recent_favorites,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "homescreen_recently_viewed_horiz" THEN a.visit_id ELSE NULL END) AS homescreen_recently_viewed_horiz,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "sdl_category_page" THEN a.visit_id ELSE NULL END) AS sdl_category_page,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "space_page" THEN a.visit_id ELSE NULL END) AS space_page,
COUNT(DISTINCT CASE WHEN a.beacon.event_name = "gift_mode_gift_idea" THEN a.visit_id ELSE NULL END) AS gift_mode_gift_idea,
COUNT(CASE WHEN a.beacon.event_name = "boe_listing_screen_similar_listings_organic_only" THEN a.visit_id ELSE NULL END) AS boe_listing_screen_similar_listings_organic_only,
COUNT(CASE WHEN a.beacon.event_name = "view_favorites_recommendations" THEN a.visit_id ELSE NULL END) AS view_favorites_recommendations,
COUNT(CASE WHEN a.beacon.event_name = "homescreen_recent_favorites" THEN a.visit_id ELSE NULL END) AS homescreen_recent_favorites,
COUNT(CASE WHEN a.beacon.event_name = "homescreen_recently_viewed_horiz" THEN a.visit_id ELSE NULL END) AS homescreen_recently_viewed_horiz,
COUNT(CASE WHEN a.beacon.event_name = "sdl_category_page" THEN a.visit_id ELSE NULL END) AS sdl_category_page,
COUNT(CASE WHEN a.beacon.event_name = "space_page" THEN a.visit_id ELSE NULL END) AS space_page,
COUNT(CASE WHEN a.beacon.event_name = "gift_mode_gift_idea" THEN a.visit_id ELSE NULL END) AS gift_mode_gift_idea,
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
WHERE DATE(_PARTITIONTIME) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
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
AND DATE(_PARTITIONTIME) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
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
AND DATE(_PARTITIONTIME) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
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
WHERE a._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
AND event_type IN ("boe_listing_screen_similar_listings_organic_only","view_favorites_recommendations", "homescreen_recent_favorites" , "homescreen_recently_viewed_horiz","sdl_category_page", "space_page", "gift_mode_gift_idea","boe_listing_screen_similar_listings_organic_only" , "view_favorites_recommendations", "homescreen_recent_favorites","homescreen_recently_viewed_horiz", "sdl_category_page", "space_page" ,"gift_mode_gift_idea","boe_landing_page_listings","boe_sdl_landing_page")
GROUP BY ALL
;



