WITH find_first_bucketed_event AS (
SELECT
  visit_id,
  MIN(sequence_number) AS first_event_sequence
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` a 
WHERE DATE(_PARTITIONTIME) BETWEEN CURRENT_DATE - 2 AND CURRENT_DATE
AND a.beacon.event_name IN (
  "boe_listing_screen_similar_listings_organic_only",
  "view_favorites_recommendations", 
  "homescreen_recent_favorites" ,
  "homescreen_recently_viewed_horiz",
  "sdl_category_page", 
  "space_page",
  "boe_listing_screen_similar_listings_organic_only",
  "view_favorites_recommendations",
  "homescreen_recent_favorites",
  "homescreen_recently_viewed_horiz",
  "sdl_category_page","space_page",
  "boe_landing_page_listings",
  "boe_sdl_landing_page")
GROUP BY ALL
)
SELECT
a.visit_id,
b.sequence_number,
b.beacon.event_name
FROM find_first_bucketed_event a
JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  ON  a.visit_id = b.visit_id 
  AND a.first_event_sequence = b.sequence_number
WHERE b.beacon.event_name IN (
      "boe_listing_screen_similar_listings_organic_only",
      "view_favorites_recommendations", 
      "homescreen_recent_favorites" ,
      "homescreen_recently_viewed_horiz",
      "sdl_category_page", 
      "space_page",
      "boe_listing_screen_similar_listings_organic_only",
      "view_favorites_recommendations",
      "homescreen_recent_favorites",
      "homescreen_recently_viewed_horiz",
      "sdl_category_page",
      "space_page",
      "boe_landing_page_listings",
      "boe_sdl_landing_page")
  AND DATE(_PARTITIONTIME) BETWEEN CURRENT_DATE - 2 AND CURRENT_DATE
;
