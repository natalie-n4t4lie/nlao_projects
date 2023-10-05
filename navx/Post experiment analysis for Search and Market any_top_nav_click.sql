BEGIN 

-- *** ENTER BELOW *** --
DECLARE config_flag_1 STRING default "navx.related_searches_nav.search"; -- enter experiment config flag
DECLARE config_flag_2 STRING default "navx.related_searches_nav.market"; -- enter experiment config flag
DECLARE start_date DATE default "2023-09-22"; -- enter experiment start date
DECLARE end_date DATE default "2023-10-01"; -- enter experiment end date

-- Bucketed visits for each variant during the experiment's timeframe
CREATE TEMP TABLE exp_visits_related_searches_nav_search
	AS
	SELECT 
		DISTINCT 
		ab_test
		,ab_variant
		,visit_id
	FROM 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
	WHERE 
		ab_test = config_flag_1
		AND _date BETWEEN start_date AND end_date
;

CREATE TEMP TABLE exp_visits_related_searches_nav_market
	AS
	SELECT 
		DISTINCT 
		ab_test
		,ab_variant
		,visit_id
	FROM 
		`etsy-data-warehouse-prod.catapult.ab_tests` e 
	WHERE 
		ab_test = config_flag_2
		AND _date BETWEEN start_date AND end_date
;

CREATE TEMP TABLE any_top_nav_click AS (
SELECT
DISTINCT visit_id
FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons` a
WHERE
DATE(TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)) BETWEEN start_date AND end_date
AND (
    (beacon.event_name = "category_page"
    -- AND (beacon.loc LIKE "%ref=catnav%" AND beacon.loc LIKE "-top-")
    )
    OR
    (beacon.event_name = "finds_page" AND beacon.loc LIKE "%cat_nav%"
    )
    OR
    (beacon.event_name = "hub" AND beacon.loc LIKE "%cat_nav%"
    )
    OR
    (beacon.event_name = "finds_page" AND beacon.loc LIKE "%gift_guide_nav_promo%"
    )
  )
)
;

SELECT
ab_test,
ab_variant,
COUNT(*) AS visit_count,
COUNT(CASE WHEN visit_id in (SELECT visit_id FROM any_top_nav_click) THEN visit_id ELSE NULL END) AS pct_with_any_top_nav_click
FROM exp_visits_related_searches_nav_search
GROUP BY 1,2
ORDER BY 2
;

SELECT
ab_test,
ab_variant,
COUNT(*) AS visit_count,
COUNT(CASE WHEN visit_id IN (SELECT visit_id FROM any_top_nav_click) THEN visit_id ELSE NULL END) AS pct_with_any_top_nav_click
FROM exp_visits_related_searches_nav_market 
GROUP BY 1,2
ORDER BY 2
;

END;

