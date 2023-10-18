WITH target_event AS (
SELECT DISTINCT 
rv.visit_id,
rv.browser_id,
rv.user_id,
event_type
FROM `etsy-data-warehouse-prod.weblog.visits` rv
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id)
WHERE rv._date = CURRENT_DATE - 2
AND rv.event_source IN ('web', 'customshops', 'craft_web')
AND is_mobile_device = 1
)
SELECT
case when rv.user_id is null then 0 else 1 end as is_sign_in,
COUNT(DISTINCT rv.visit_id) AS visit_count,
COUNT(DISTINCT rv.browser_id) AS browser_count,
COUNT(DISTINCT rv.user_id) AS user_count,
SUM(total_gms) AS gms_count,
COUNT(DISTINCT CASE WHEN visit_id in (select visit_id from target_event where event_type = 'mobile_category_nav_open') THEN visit_id ELSE NULL END) AS visit_open_nav,
COUNT(DISTINCT CASE WHEN browser_id in (select browser_id from target_event where event_type = 'mobile_category_nav_open') THEN browser_id ELSE NULL END) AS browser_open_nav,
COUNT(DISTINCT CASE WHEN user_id in (select user_id from target_event where event_type = 'mobile_category_nav_open') THEN user_id ELSE NULL END) AS user_open_nav,
SUM(CASE WHEN visit_id in (select visit_id from target_event where event_type = 'mobile_category_nav_open') THEN total_gms ELSE NULL END) AS gms_open_nav,
FROM `etsy-data-warehouse-prod.weblog.visits` rv
WHERE rv._date = CURRENT_DATE - 2
AND rv.event_source IN ('web', 'customshops', 'craft_web')
AND is_mobile_device = 1
GROUP BY 1
;
