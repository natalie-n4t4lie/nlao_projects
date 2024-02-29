-- Q: How many percent of iOS BOE visits visited home tab at least once during their visit?
WITH visit_classification AS (
SELECT
visit_id,
browser_id,
user_id,
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform,
CASE WHEN buyer_segment IS NOT NULL THEN buyer_segment ELSE "Sign-out" END AS buyer_segment
FROM `etsy-data-warehouse-prod.weblog.visits` v
LEFT JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_profile` u
  on v.user_id = u.mapped_user_id
WHERE _date >= current_date - 30
)
SELECT
buyer_segment,
count(distinct visit_id) as visit_count,
count(distinct case when event_type = "homescreen" then visit_id else null end) as homescreen_visit_count,
count(distinct case when event_type = "homescreen" then visit_id else null end) / count(distinct visit_id) as pct_visit_view_homescreen
FROM visit_classification
LEFT JOIN `etsy-data-warehouse-prod.weblog.events`
  USING (visit_id)
WHERE app_platform = 'boe ios'
GROUP BY 1
;

-- Q: How many percent of iOS BOE visits visited home tab more than once during their visit? What’s the average number of homescreen visits within a visit?
WITH visit_classification AS (
SELECT
v.visit_id,
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform,
CASE WHEN buyer_segment IS NOT NULL THEN buyer_segment ELSE "Sign-out" END AS buyer_segment,
COALESCE(SUM(case when event_type = "homescreen" then 1 else null end),0) AS homescreen_visit_count
FROM `etsy-data-warehouse-prod.weblog.visits` v
LEFT JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_profile` u
  ON v.user_id = u.mapped_user_id
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON v.visit_id = e.visit_id
WHERE e._date >= current_date - 30
AND v._date >= current_date - 30
GROUP BY 1,2,3
)
SELECT
CASE WHEN homescreen_visit_count>=2 THEN '2+'
      WHEN homescreen_visit_count = 1 THEN '1' 
      WHEN homescreen_visit_count = 0 THEN '0'
      END AS homescreen_visit_count_group,
count(distinct visit_id) AS visit_count
FROM visit_classification
WHERE app_platform = 'boe ios'
GROUP BY 1
;

WITH visit_classification AS (
SELECT
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform,
v.visit_id,
COALESCE(SUM(case when event_type = "homescreen" then 1 else null end),0) AS homescreen_visit_count
FROM `etsy-data-warehouse-prod.weblog.visits` v
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON v.visit_id = e.visit_id
WHERE e._date >= current_date - 30
AND v._date >= current_date - 30
GROUP BY 1,2
)
SELECT
avg(homescreen_visit_count) as avg_homescreen_visit_count
FROM visit_classification
WHERE app_platform = 'boe ios'
;

-- Q: What % of App home users visits homescreen more than once a week? More than once a day?
WITH visit_classification AS (
SELECT DISTINCT
v.user_id,
v.browser_id,
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform,
v.visit_id,
v._date
FROM `etsy-data-warehouse-prod.weblog.visits` v
WHERE v._date >= current_date - 30
)
SELECT
COUNT(DISTINCT browser_id) AS browser_count,
COUNT(DISTINCT user_id) as user_count
FROM visit_classification
WHERE app_platform = 'boe ios'
;

WITH visit_classification AS (
SELECT
v.user_id,
v.browser_id,
 CASE WHEN event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 0 THEN 'desktop'
          when event_source IN ('web', 'customshops', 'craft_web')
          AND is_mobile_device = 1 THEN  'mobile_web'
          when event_source IN ('ios','android')
          AND REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'soe'
          when event_source IN ('ios')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe ios'
          when event_source IN ('android')
          AND NOT REGEXP_CONTAINS(LOWER(user_agent), LOWER('SellOnEtsy')) THEN 'boe android'
          ELSE'undefined' END AS app_platform,
v.visit_id,
v._date
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e
  ON v.visit_id = e.visit_id
WHERE e._date >= current_date - 30
AND v._date >= current_date - 30
AND event_type = "homescreen"
ORDER BY 1, 3 ASC
)
, date_diffs AS (
SELECT
user_id,
browser_id,
visit_id,
_date AS current_homescreen_visit_date,
LEAD(_date) OVER (PARTITION BY browser_id ORDER BY _date) AS previous_homescreen_visit_date_browser,
date_diff(LEAD(_date) OVER (PARTITION BY browser_id ORDER BY _date), _date,DAY) AS homescreen_daydiff_browser,
LEAD(_date) OVER (PARTITION BY user_id ORDER BY _date) AS previous_homescreen_visit_date_user,
date_diff(LEAD(_date) OVER (PARTITION BY user_id ORDER BY _date), _date,DAY) AS homescreen_daydiff_user,
FROM visit_classification
WHERE app_platform = 'boe ios'
)
SELECT
COUNT(DISTINCT CASE WHEN homescreen_daydiff_browser BETWEEN 1 and 7 THEN browser_id ELSE NULL END) AS sameweek_homescreen_browser_count,
COUNT(DISTINCT CASE WHEN homescreen_daydiff_user BETWEEN 1 and 7 THEN user_id ELSE NULL END) AS sameweek_homescreen_user_count,
COUNT(DISTINCT CASE WHEN homescreen_daydiff_browser BETWEEN 0 and 7 THEN browser_id ELSE NULL END) AS sameweekday_homescreen_browser_count,
COUNT(DISTINCT CASE WHEN homescreen_daydiff_user BETWEEN 0 and 7 THEN user_id ELSE NULL END) AS sameweekday_homescreen_user_count,
COUNT(DISTINCT CASE WHEN homescreen_daydiff_browser =0 THEN browser_id ELSE NULL END) AS sameday_homescreen_browser_count,
COUNT(DISTINCT CASE WHEN homescreen_daydiff_user =0 THEN user_id ELSE NULL END) AS sameday_homescreen_user_count,
FROM date_diffs
;



