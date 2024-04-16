DECLARE platform_list ARRAY<STRING> DEFAULT ["ios", "android"];
DECLARE lookback_date DEFAULT CURRENT_DATE() - 7;

-- ## Current data refresh volume
WITH cte AS(
SELECT
       e._date,
       count(*) AS event_count
FROM `etsy-data-warehouse-prod.weblog.events` e
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
  ON e.visit_id = v.visit_id
  WHERE
      e.event_type = "boe_homescreen_tab_delivered"
      AND v.platform = "boe"
      AND v.event_source IN UNNEST(platform_list)
      AND v._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
      AND e._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
GROUP BY 1
)
SELECT
ROUND(AVG(event_count),0) AS avg_data_refresh_ct
FROM cte
;

-- ## Proposed incremental volume increase

-- ##Assuming data refresh eligiblity check happens when user first visit homepage during their visit
-- ##Find the first homepage visit timestamp for a given visit_id
WITH home_page_visit AS (
   SELECT DISTINCT
       v._date AS visit_date,
       v.user_id,
       v.browser_id,
       v.visit_id,
       v.start_datetime,
       MIN(e.epoch_ms) AS homepage_visit_epoch_ms,
   FROM `etsy-data-warehouse-prod.weblog.recent_visits` v
   JOIN 
        (SELECT * 
        FROM `etsy-data-warehouse-prod.weblog.events` e
        WHERE e.event_type = "homescreen"
              AND e._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
        ) e
    ON e.visit_id = v.visit_id
   WHERE
    v.platform = "boe"
       AND v.event_source IN UNNEST(platform_list)
       AND v._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
    GROUP BY 1,2,3,4,5
),
-- ## Get the lastest timestamp of homepage data refresh for each browser_id
homepage_refresh AS (
   SELECT DISTINCT
       v._date AS visit_date,
       v.user_id,
       v.browser_id,
       v.visit_id,
       v.start_datetime,
       MAX(e.epoch_ms) AS last_homepage_refresh_epoch_ms,
   FROM `etsy-data-warehouse-prod.weblog.recent_visits` v
   LEFT JOIN 
        (SELECT * 
        FROM `etsy-data-warehouse-prod.weblog.events` e
        WHERE e.event_type = "boe_homescreen_tab_delivered"
              AND e._date BETWEEN DATE_SUB(lookback_date, INTERVAL 14 DAY) AND lookback_date --extending look back further to account for pre-period data refresh
        ) e
    ON e.visit_id = v.visit_id
   WHERE
    v.platform = "boe"
       AND v.event_source IN UNNEST(platform_list)
       AND v._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
    GROUP BY 1,2,3,4,5
)
--# When there is a last homepage refresh timestamp, compare the time difference with home page visit timestamp, 
--# When there isn't homepage refresh timestamp, set homepage refresh value to an arbituary date from 14 days ago to ensure that visit is qualified for all data refresh
, data_refresh_eligibility AS (
SELECT
       v.visit_date,
       v.user_id,
       v.browser_id,
       v.start_datetime,
       v.visit_id,
       homepage_visit_epoch_ms,
       MAX(CASE WHEN TIMESTAMP_DIFF(timestamp_millis(homepage_visit_epoch_ms),IFNULL(timestamp_millis(last_homepage_refresh_epoch_ms), DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)),HOUR) >= 3 THEN 1 ELSE 0 END) AS eligible_for_3hr_refresh,
       MAX(CASE WHEN TIMESTAMP_DIFF(timestamp_millis(homepage_visit_epoch_ms),IFNULL(timestamp_millis(last_homepage_refresh_epoch_ms), DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)),HOUR) >= 6 THEN 1 ELSE 0 END) AS eligible_for_6hr_refresh,
       MAX(CASE WHEN TIMESTAMP_DIFF(timestamp_millis(homepage_visit_epoch_ms),IFNULL(timestamp_millis(last_homepage_refresh_epoch_ms), DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)),HOUR) >= 12 THEN 1 ELSE 0 END) AS eligible_for_12hr_refresh,
       MAX(CASE WHEN TIMESTAMP_DIFF(timestamp_millis(homepage_visit_epoch_ms),IFNULL(timestamp_millis(last_homepage_refresh_epoch_ms), DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)),HOUR) >= 24 THEN 1 ELSE 0 END) AS eligible_for_24hr_refresh,
FROM home_page_visit v
LEFT JOIN homepage_refresh r
    ON v.browser_id = r.browser_id AND timestamp_millis(v.homepage_visit_epoch_ms) > IFNULL(timestamp_millis(r.last_homepage_refresh_epoch_ms), DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY))
GROUP BY 1,2,3,4,5,6
)
, cal AS (
SELECT
visit_date,
eligible_for_3hr_refresh,
eligible_for_6hr_refresh,
eligible_for_12hr_refresh,
eligible_for_24hr_refresh,
COUNT(*) AS distinct_visit_id_ct,
COUNT(DISTINCT browser_id) AS distinct_browser_id_ct,
COUNT(DISTINCT user_id) AS distinct_user_id_ct,
FROM data_refresh_eligibility
GROUP BY 1,2,3,4,5
)
SELECT 
eligible_for_3hr_refresh,
eligible_for_6hr_refresh,
eligible_for_12hr_refresh,
eligible_for_24hr_refresh,
ROUND(avg(distinct_visit_id_ct),0) AS avg_visit_ct,
ROUND(avg(distinct_browser_id_ct),0) AS avg_browser_ct,
ROUND(avg(distinct_user_id_ct),0) AS avg_user_ct,
FROM cal
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
;

DECLARE platform_list ARRAY<STRING> DEFAULT ["ios", "android"];
DECLARE lookback_date DEFAULT CURRENT_DATE() - 7;

WITH cte AS (
   SELECT
       v._date AS visit_date,
       COUNT(DISTINCT v.visit_id) visit_ct,
       COUNT(DISTINCT v.browser_id) browser_ct,
       COUNT(DISTINCT v.user_id) user_ct,
   FROM `etsy-data-warehouse-prod.weblog.recent_visits` v
   WHERE
    v.platform = "boe"
       AND v.event_source IN UNNEST(platform_list)
       AND v._date BETWEEN DATE_SUB(lookback_date, INTERVAL 6 DAY) AND lookback_date
    GROUP BY 1
)
SELECT
AVG(visit_ct) as avg_visit,
AVG(browser_ct) as avg_browser,
AVG(user_ct) as avg_browser,
FROM cte
;
