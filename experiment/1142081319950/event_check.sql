-- Fires when "Add to cart" / "listing" / "shop_home" button is delivered on updates tab
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


-- Fires when user saw "Add to cart" / "listing" / "shop_home" button on updates tab
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

-- Fires when user tapped "Add to cart" / "listing" / "shop_home"  button on updates tab
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
