-- Check event firing for [iOS] Complete the Look on Home v1 (https://atlas.etsycorp.com/catapult/1130656201364)

-- DELIVERED MODULE EVENT CHECK
SELECT
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) <= current_date
and beacon.event_name = "recommendations_module_delivered"
and (select value from unnest(beacon.properties.key_value) where key = "module_placement") like "%ctl_%"
group by 1
;-- no?

-- SEEN MODULE EVENT CHECK
SELECT
(select value from unnest(beacon.properties.key_value) where key = "module_placement") AS module_placement,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) <= current_date
and beacon.event_name = "recommendations_module_seen"
and (select value from unnest(beacon.properties.key_value) where key = "module_placement") like "%boe_homescreen_ctl_prior_purchase%"
group by 1
;

-- TAP MODULE EVENT CHECK
SELECT
split((select value from unnest(beacon.properties.key_value) where key = "content_source"),"-")[offset(0)] AS content_source,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) <= current_date
and beacon.event_name = "homescreen_tapped_listing"
and (select value from unnest(beacon.properties.key_value) where key = "content_source") like "%boe_homescreen_ctl_prior_purchase%"
group by 1
;
