-- Check event firing for [iOS] Add Buy Now to “something in your cart is now on sale” Update (https://atlas.etsycorp.com/catapult/1141006124358)

-- Fires when user tapped on listing from "Something in your cart is on sale" on updates tab
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

-- Fires when user visit notification tab with 0/1-4/5-10/11+ update
SELECT
CASE WHEN CAST((select value from unnest(beacon.properties.key_value) where key = "total_updates_count") AS INT64) = 0 THEN "0"
     WHEN CAST((select value from unnest(beacon.properties.key_value) where key = "total_updates_count") AS INT64) BETWEEN 1 AND 4 THEN "1-4"
     WHEN CAST((select value from unnest(beacon.properties.key_value) where key = "total_updates_count") AS INT64) BETWEEN 5 AND 10 THEN "5-10"
     WHEN CAST((select value from unnest(beacon.properties.key_value) where key = "total_updates_count") AS INT64) >10 THEN "11+"
     END AS update_count,
count(*) AS visit_count
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` a
where
date(_partitiontime) BETWEEN DATE_SUB(current_date, INTERVAL 14 DAY) AND CURRENT_DATE
and beacon.event_name = "notification_tab_delivered"
group by 1
;
