
-- FOR A GIVEN EVENT, FIND ALL PROPERTIES
SELECT distinct k.key,
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b,
  UNNEST(beacon.properties.key_value) AS k
WHERE b.beacon.event_name = "deals_tab_tapped_listing"
AND DATE(_PARTITIONTIME) = CURRENT_DATE - 2
LIMIT 100
;

-- FOR A GIVEN PROPERTIES, FIND ALL VALUES
SELECT distinct k.key,
k.value
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` b,
  UNNEST(beacon.properties.key_value) AS k
WHERE b.beacon.event_name = "deals_tab_tapped_listing"
AND DATE(_PARTITIONTIME) = CURRENT_DATE - 2
AND k.key = 'franz_host' --> INSERT PROPERTIES NAME
limit 100
;
