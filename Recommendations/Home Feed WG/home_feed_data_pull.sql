CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.home_feed_data` AS (
SELECT
    DISTINCT DATE(_partitiontime) AS _date
    , visit_id
    , beacon.browser_id
    , beacon.user_id
    , beacon.event_name
    , CASE WHEN beacon.event_name IN ("boe_homescreen_feed_delivered","boe_homescreen_feed_seen") 
              THEN (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "content_source_uid")
           WHEN beacon.event_name = "homescreen_tapped_listing" AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "content_source") LIKE "%boe_homescreen_feed%"
              THEN SPLIT((SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "content_source"),"-")[safe_offset(1)] END  
     AS content_source_uid
    , CASE WHEN beacon.event_name = "homescreen_tapped_listing" THEN (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_id") END AS listing_id
    , sequence_number
  FROM
    `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  WHERE DATE(_partitiontime) BETWEEN CURRENT_DATE - 44 AND CURRENT_DATE - 30
   AND (
    beacon.event_name = "boe_homescreen_feed_delivered"
    OR beacon.event_name = 'boe_homescreen_feed_seen'
    OR (beacon.event_name = "homescreen_tapped_listing" 
        AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "content_source") LIKE "%boe_homescreen_feed%")
    )
);


SELECT DISTINCT listing_id FROM `etsy-data-warehouse-dev.nlao.home_feed_data` LIMIT 10;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.home_feed_engagement` AS ( 
WITH seen AS (
  SELECT
    s.visit_id
    , s.content_source_uid
    , s.sequence_number AS seen_sequence
    , MAX(d.sequence_number) AS max_delivered_sequence 
  FROM
    `etsy-data-warehouse-dev.nlao.home_feed_data` d
  LEFT JOIN (
    SELECT
      *
    FROM
      `etsy-data-warehouse-dev.nlao.home_feed_data`
    WHERE
      event_name = "boe_homescreen_feed_seen") s
  ON
    d.visit_id = s.visit_id
    AND d._date = s._date
    AND d.content_source_uid = s.content_source_uid
    AND d.sequence_number < s.sequence_number --seen happens after delivery
  WHERE
    d.event_name = "boe_homescreen_feed_delivered"
  GROUP BY ALL
  ),
  clicked AS (
    SELECT
    s.visit_id
    , s.content_source_uid
    , s.sequence_number AS click_sequence
    , MAX(d.sequence_number) AS max_delivered_sequence 
  FROM
    `etsy-data-warehouse-dev.nlao.home_feed_data` d
  LEFT JOIN (
    SELECT
      *
    FROM
      `etsy-data-warehouse-dev.nlao.home_feed_data`
    WHERE
      event_name = "homescreen_tapped_listing") s
  ON
    d.visit_id = s.visit_id
    AND d._date = s._date
    AND d.content_source_uid = s.content_source_uid
    AND d.sequence_number < s.sequence_number --seen happens after delivery
  WHERE
    d.event_name = "boe_homescreen_feed_delivered"
  GROUP BY ALL
  )
  SELECT
    d._date,
    d.visit_id,
    d.browser_id,
    d.user_id,
    d.sequence_number,
    MAX(CASE WHEN s.seen_sequence IS NOT NULL OR c.click_sequence IS NOT NULL THEN 1 ELSE 0 END) AS seen,
    MAX(CASE WHEN c.click_sequence IS NOT NULL THEN 1 ELSE 0 END) AS clicked
  FROM `etsy-data-warehouse-dev.nlao.home_feed_data` d
  LEFT JOIN seen s
    ON d.visit_id = s.visit_id 
      AND d.content_source_uid = s.content_source_uid
      AND s.seen_sequence > d.sequence_number
  LEFT JOIN clicked c
    ON d.visit_id = c.visit_id 
      AND d.content_source_uid = c.content_source_uid
      AND c.click_sequence > d.sequence_number
  GROUP BY ALL
)
;

-- CTR
SELECT 
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_ct,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_ct,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) / COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS ctr,
COUNT(DISTINCT CASE WHEN seen = 1 THEN user_id ELSE NULL END) AS user_seen_ct,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN user_id ELSE NULL END) AS user_clicked_ct,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN user_id ELSE NULL END) / COUNT(DISTINCT CASE WHEN seen = 1 THEN user_id ELSE NULL END) AS user_ctr
FROM `etsy-data-warehouse-dev.nlao.home_feed_engagement` 
;

-- HOME FEED POST CLICK PURCHASE RATE (PCPR)
WITH cte AS (
  SELECT DISTINCT
  visit_id,
  listing_id,
  sequence_number
  FROM `etsy-data-warehouse-dev.nlao.home_feed_data` e
  WHERE e.event_name = "homescreen_tapped_listing"
),
listing_views AS(
  SELECT DISTINCT 
  a._date,
  a.visit_id,
  a.platform,
  a.ref_tag,
  a.listing_id,
  a.sequence_number,
  a.favorited,
  a.added_to_cart,
  a.purchased_after_view
FROM
  `etsy-data-warehouse-prod.analytics.listing_views` AS a
WHERE
  -- a.run_date BETWEEN UNIX_SECONDS(CAST(CURRENT_DATE - 44 AS TIMESTAMP)) AND UNIX_SECONDS(CAST(CURRENT_DATE - 30 AS TIMESTAMP))
  -- AND 
  a.platform IN('desktop','mobile_web','boe')
  AND a._date BETWEEN CURRENT_DATE - 44 AND CURRENT_DATE - 30
)
,purchase_cal AS (
SELECT
  c.listing_id,
  c.visit_id,
  c.sequence_number,
  MAX(a.purchased_after_view) AS purchased
FROM listing_views AS a
JOIN cte c
  ON a.visit_id = c.visit_id
  AND a.listing_id = CAST(c.listing_id AS INT64)
  AND a.sequence_number > c.sequence_number
GROUP BY ALL
)
SELECT
  SUM(purchased)/COUNT(listing_id) AS pcpr
FROM purchase_cal
;-- 0.001057629842108732

-- HOMESCREEN PCPR
  SELECT 
    SUM(purchased_after_view)/SUM(clicked) AS pcpr
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` e
  WHERE e.module_page = 'home' 
    AND e.clicked = 1 
    AND _date BETWEEN CURRENT_DATE - 44 AND CURRENT_DATE - 30
;--0.030096221028886271

-- RE-ENGAGEMENT IN 30 DAYS
SELECT
COUNT(DISTINCT e1.user_id) AS feed_engaged_user_ct,
COUNT(DISTINCT e2.user_id) AS feed_reengaged_user_ct,
COUNT(DISTINCT e2.user_id)/COUNT(DISTINCT e1.user_id) AS reengagement_rate
FROM `etsy-data-warehouse-dev.nlao.home_feed_engagement` e1
LEFT JOIN `etsy-data-warehouse-dev.nlao.home_feed_engagement` e2
  ON e1.user_id = e2.user_id
  AND e1.visit_id != e2.visit_id
  AND e2._date > e1._date AND e2._date < DATE_ADD(e1._date, INTERVAL 30 DAY)
  AND e1.clicked = e2.clicked
WHERE e1.clicked = 1
;

WITH home_page_visit AS (
SELECT DISTINCT
e._date,
v.user_id,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` e
JOIN `etsy-data-warehouse-prod.weblog.visits` v
  ON v.visit_id = e.visit_id
WHERE e.clicked = 1 AND e.module_page = "home"
AND v._date BETWEEN CURRENT_DATE - 44 AND CURRENT_DATE - 30
)
SELECT
COUNT(DISTINCT e1.user_id) AS feed_engaged_user_ct,
COUNT(DISTINCT e2.user_id) AS feed_reengaged_user_ct,
COUNT(DISTINCT e2.user_id)/COUNT(DISTINCT e1.user_id) AS reengagement_rate
FROM home_page_visit e1
LEFT JOIN home_page_visit e2
  ON e1.user_id = e2.user_id
  AND e2._date > e1._date AND e2._date < DATE_ADD(e1._date, INTERVAL 30 DAY)
;
