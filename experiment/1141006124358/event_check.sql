-- Check event firing for [iOS] Add ATC button next to “an item you favourited is now on sale” (https://atlas.etsycorp.com/catapult/1163429430255)

-- Fires when user tapped on listing from "An item you favourited is now on sale" on updates tab
SELECT
(select value from unnest(beacon.properties.key_value) where key = "type") AS module_placement,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_tapped_listing"
and (select value from unnest(beacon.properties.key_value) where key = "type") = "clos"
group by 1
;

-- Fires when user visit notification tab
SELECT
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_tab_delivered"
;

-- Fires when "Add to cart" / "listing" button is delivered on updates tab
SELECT
(select value from unnest(beacon.properties.key_value) where key = "formatted_button_type") as formatted_button_type,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_formatted_button_delivered"
group by 1
;


-- Fires when user saw "Add to cart" / "listing"  button on updates tab
SELECT
(select value from unnest(beacon.properties.key_value) where key = "formatted_button_type") as formatted_button_type,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_formatted_button_seen"
group by 1
;

-- Fires when user tapped "Add to cart" / "listing"  button on updates tab
SELECT
(select value from unnest(beacon.properties.key_value) where key = "formatted_button_type") as formatted_button_type,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_formatted_button_tapped"
group by 1
;
