-- Check event firing for [iOS] Guided In-Line Search Reformulation Suggestions v2 (https://atlas.etsycorp.com/catapult/1130648687664)

-- DELIVERED MODULE EVENT CHECK
SELECT
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "recommendations_module_delivered"
and (select value from unnest(beacon.properties.key_value) where key = "module_placement") like "%boe_search_screen_human_curated_guided_search%"
group by 1
;

-- SEEN MODULE EVENT CHECK
SELECT
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "recommendations_module_seen"
and (select value from unnest(beacon.properties.key_value) where key = "module_placement") like "%boe_search_screen_human_curated_guided_search%"
group by 1
;

-- TAP MODULE EVENT CHECK
SELECT
split((select value from unnest(beacon.properties.key_value) where key = "content_source"),"-")[offset(0)] AS content_source,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "homescreen_tapped_listing"
and (select value from unnest(beacon.properties.key_value) where key = "content_source") like "%boe_search_screen_human_curated_guided_search%"
group by 1
;
