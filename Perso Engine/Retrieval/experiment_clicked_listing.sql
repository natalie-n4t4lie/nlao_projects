-- INPUT
DECLARE config_flag_param STRING DEFAULT "ranking/recs.boe_similar_l2l_coclicks";
DECLARE target_module_placement STRING DEFAULT "boe_listing_screen_based_on_recent_views";
-- DON'T CHANGE
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;

DECLARE bucketing_id_type INT64;

IF start_date IS NULL OR end_date IS NULL THEN
    SET (start_date, end_date) = (
        SELECT AS STRUCT
            MAX(DATE(boundary_start_ts)) AS start_date,
            MAX(_date) AS end_date,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            experiment_id = config_flag_param
    );
END IF;

IF is_event_filtered IS NULL THEN
    SET (is_event_filtered, bucketing_id_type) = (
        SELECT AS STRUCT
            is_filtered,
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
ELSE
    SET bucketing_id_type = (
        SELECT
            bucketing_id_type,
        FROM
            `etsy-data-warehouse-prod.catapult_unified.experiment`
        WHERE
            _date = end_date
            AND experiment_id = config_flag_param
    );
END IF;

-- TIPS:
--   - Replace 'nlao' in the table names below with your own username or personal dataset name.
--   - Additionally, there are a few TODO items in this script depending on:
--       - Whether you would like to look at certain segmentations  (marked with <SEGMENTATION>)
--       - Whether you would like to look at certain events         (marked with <EVENT>)
--     Before running, please review the script and adjust the marked sections accordingly!

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type AS bucketing_id_type,
        variant_id,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.bucketing`
    WHERE
        _date BETWEEN start_date AND end_date
        AND experiment_id = config_flag_param
    GROUP BY
        1,2,3
);

IF is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            MIN(f.event_ts) AS bucketing_ts,
        FROM
            `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            USING(bucketing_id)
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
            AND f.event_ts >= f.boundary_start_ts
            AND f.event_ts >= a.bucketing_ts
        GROUP BY
            bucketing_id, bucketing_id_type, variant_id
    );
END IF;

IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
            v.user_id
        FROM
            `etsy-data-warehouse-dev.nlao.ab_first_bucket` b
        JOIN
            `etsy-data-warehouse-prod.weblog.visits` v
            ON b.bucketing_id = v.browser_id
            AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        WHERE
            v._date BETWEEN start_date AND end_date
    );

ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
            v.user_id
        FROM
            `etsy-data-warehouse-dev.nlao.ab_first_bucket` b
        JOIN
            `etsy-data-warehouse-prod.weblog.visits` v
            -- Note that for user experiments, you may miss out on some visits in cases where multiple
            -- users share the same visit_id. This is because only the first user_id is recorded in
            -- the weblog.visits table (as of Q4 2023).
            --
            -- Additionally, the only difference between the user and browser case is the join on
            -- bucketing_id. However, due to performance reasons, we apply our conditional logic at
            -- a higher level rather than in the join itself.
            ON b.bucketing_id = CAST(v.user_id AS STRING)
            AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        WHERE
            v._date BETWEEN start_date AND end_date
    );
END IF;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.experiment_clicked_listings` AS (
SELECT
v.bucketing_id,
v.variant_id,
v.visit_id,
r.module_placement,
r.dataset,
r.fallback_datasets,
r.candidate_set,	
r.target_listing_id,
r.listing_id,
r.target_top_category,
r.rec_top_category,
r.target_taxonomy_id,
r.rec_taxonomy_id,
r.target_price,
r.rec_price,
r.seen,
r.clicked,
r.purchased_after_view
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
  ON v.visit_id = r.visit_id
WHERE module_placement = target_module_placement
  AND clicked = 1
)
;
