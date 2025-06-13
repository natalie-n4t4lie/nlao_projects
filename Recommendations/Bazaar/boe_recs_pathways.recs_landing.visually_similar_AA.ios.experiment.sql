/*
Note: This script is intended to analyze custom date ranges for Catapult experiments
and thus relies on more "raw" forms of experiment data.
If you are not interested in custom date ranges (i.e. you want to analyze the experiment boundaries
as defined in Catapult), please use the catapult_unified_event_level_data_simplified.sql instead,
which uses pre-aggregated data that allows the script to run faster.
This script contains queries on the catapult_unified tables that can be used for analyzing experiment results.
It takes in an experiment (config flag) and date range as input, and provides the following data:
* First bucketing moment for each experimental unit (i.e. browser or user)
* Segment values for each experimental unit
* Event counts, GMS, and visit count for each experimental unit
* Combined data for all events and experimental units, with segment values
* Query to recreate the metric results from the Catapult UI
If you want to join catapult data with non-catapult tables that rely on visit ids,
this script also includes example queries on how to get visit ids for your experiment.
For more details on how to work with catapult_unified tables, go to go/catapult-unified-docs
*/

-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "boe_recs_pathways.recs_landing.visually_similar_AA.ios.experiment";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify a custom date range, you can also specify the start and end date manually.
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";

-- By default, this script automatically detects whether the experiment is event filtered or not
-- and provides the associated analysis. However, in the case that we want to examine non-filtered
-- results for an event filtered experiment, this variable may be manually set to "FALSE".
DECLARE is_event_filtered BOOL DEFAULT FALSE;

-- Generally, this variable should not be overridden, as the grain of analysis should match the
-- bucketing ID type.
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
        bucketing_id, bucketing_id_type, variant_id
);

-- For event filtered experiments, the effective bucketing event for a bucketed unit
-- into a variant is the FIRST filtering event to occur after that bucketed unit was
-- bucketed into that variant of the experiment.
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

-------------------------------------------------------------------------------------------
-- SEGMENT DATA
-------------------------------------------------------------------------------------------
-- Get segment values based on first bucketing moment.
-- Example output:
-- bucketing_id | variant_id | event_id         | event_value
-- 123          | off        | buyer_segment    | New
-- 123          | off        | canonical_region | FR
-- 456          | on         | buyer_segment    | Habitual
-- 456          | on         | canonical_region | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.first_bucket_segments_unpivoted` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        s.event_id,
        s.event_value,
    FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    JOIN
        `etsy-data-warehouse-prod.catapult_unified.segment_event` s
        USING(bucketing_id, bucketing_ts)
    WHERE
        s._date BETWEEN start_date AND end_date
        AND s.experiment_id = config_flag_param
        -- <SEGMENTATION> Here you can specify whatever segmentations you'd like to analyze.
        -- !!! Please keep this in sync with the PIVOT statement below !!!
        -- For all supported segmentations, see go/catapult-unified-docs.
        AND s.event_id IN (
            "buyer_segment",
            "canonical_region",
            "app_version"
        )
);

-- Pivot the above table to get one row per bucketing_id and variant_id. Each additional
-- column will be a different segmentation, and the value will be the segment for each
-- bucketing_id at the time they were first bucketed into the experiment date range being
-- analyzed.
-- Example output (using the same example data above):
-- bucketing_id | variant_id | buyer_segment | canonical_region
-- 123          | off        | New           | FR
-- 456          | on         | Habitual      | US
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.first_bucket_segments` AS (
    SELECT
        *
    FROM
        `etsy-data-warehouse-dev.nlao.first_bucket_segments_unpivoted`
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment",
            "canonical_region",
            "app_version"
        )
    )
);

-------------------------------------------------------------------------------------------
-- EVENT AND GMS DATA
-------------------------------------------------------------------------------------------
-- <EVENT> Specify the events you want to analyze here.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.events` AS (
    SELECT
        *
    FROM
        UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "prolist_total_spend",  -- prolist revenue
            "gms",                   -- note: gms data is in cents
            "backend_favorite_item2",
            "boe_recs_landing",
            "listing_interaction",
            "listing_interaction_tap",
            "listing_interaction_tap_ads",
            "listing_interaction_tap_search",
            "recs_landing_listing_tapped",
            "prolist_click_full_boe_search",
            "search",
            "search_results_tapped_listing",
            "visual_search_tapped_backend"
        ]) AS event_id
);

-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.events_per_unit` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        e.event_id,
        CAST(SUM(e.event_value) AS FLOAT64) AS event_value,
    FROM
        `etsy-data-warehouse-prod.catapult_unified.event` e
    CROSS JOIN
        UNNEST(e.associated_ids) ids
    JOIN
        `etsy-data-warehouse-dev.nlao.events`
        USING(event_id)
    JOIN
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
        ON a.bucketing_id = ids.id
        AND a.bucketing_id_type = ids.id_type
    WHERE
        e._date BETWEEN start_date AND end_date
        AND e.event_type IN (1, 3, 4) -- fired, gms, and bounce events (see go/catapult-unified-enums)
        AND e.event_ts >= a.bucketing_ts
    GROUP BY
        bucketing_id, variant_id, event_id
);

-- Insert custom events separately, as custom event data does not exist in the event table (as of Q4 2023).
IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                v.visit_id,
                a.variant_id,
                a.bucketing_ts,
                v.sequence_number,
                v.event_name AS event_id,
                v.event_data AS event_value,
                v.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.visit_segment_custom_metrics` v
                ON a.bucketing_id = SPLIT(v.visit_id, '.')[OFFSET(0)]
            WHERE
                v._date BETWEEN start_date AND end_date
                AND v.event_timestamp >= a.bucketing_ts
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.nlao.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.nlao.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND sequence_number = 0)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.post_bucketing_custom_events` AS (
        WITH custom_events AS (
            SELECT
                a.bucketing_id,
                c.visit_id,
                a.variant_id,
                a.bucketing_ts,
                c.sequence_number,
                c.event_name AS event_id,
                c.event_data AS event_value,
                c.event_timestamp,
            FROM
                `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
            JOIN
                `etsy-data-warehouse-prod.catapult.custom_events_by_user_slice` c
                ON a.bucketing_id = c.user_id
            WHERE
                c._date BETWEEN start_date AND end_date
                AND c.event_timestamp >= a.bucketing_ts
        )
        SELECT
            bucketing_id,
            visit_id,
            variant_id,
            bucketing_ts,
            sequence_number,
            event_id,
            event_value,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number,
            ROW_NUMBER() OVER (
                PARTITION BY bucketing_id, variant_id, event_id, visit_id
                ORDER BY sequence_number
            ) AS row_number_in_visit,
        FROM
            custom_events
    );

    INSERT INTO `etsy-data-warehouse-dev.nlao.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.nlao.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND row_number_in_visit = 1)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
END IF;

-------------------------------------------------------------------------------------------
-- VISIT COUNT
-------------------------------------------------------------------------------------------

-- Get all post-bucketing visits for each experimental unit
IF bucketing_id_type = 1 THEN -- browser data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
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

-- Get visit count per experimental unit
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.visits_per_unit` AS (
    SELECT
        bucketing_id,
        variant_id,
        COUNT(*) AS visit_count,
    FROM
        `etsy-data-warehouse-dev.nlao.subsequent_visits`
    GROUP BY
        bucketing_id, variant_id
);

-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA
-------------------------------------------------------------------------------------------
-- All events for all bucketed units, with segment values.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.all_units_events_segments` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        COALESCE(event_value, 0) AS event_count,
        buyer_segment,
        canonical_region,
    FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket`
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
    JOIN
        `etsy-data-warehouse-dev.nlao.first_bucket_segments`
        USING(bucketing_id, variant_id)
);

-------------------------------------------------------------------------------------------
-- RECREATE CATAPULT RESULTS
-------------------------------------------------------------------------------------------
-- Proportion and mean metrics by variant and event_name
SELECT
    event_id,
    variant_id,
    COUNT(*) AS total_units_in_variant,
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event
FROM
    `etsy-data-warehouse-dev.nlao.all_units_events_segments`
GROUP BY
    event_id, variant_id
ORDER BY
    event_id, variant_id;

-------------------------------------------------------------------------------------------
-- VISIT IDS TO JOIN WITH EXTERNAL TABLES
-------------------------------------------------------------------------------------------
-- Need visit ids to join with non-Catapult tables?
-- No problem! Here are some examples for how to get the visit ids for each experimental unit.

-- All associated IDs in the bucketing visit
IF NOT is_event_filtered THEN
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(b.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.bucketing` b
            USING(bucketing_id, variant_id, bucketing_ts)
        WHERE
            b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    );
ELSE
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            a.bucketing_ts,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 4) AS sequence_number,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 1) AS browser_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 2) AS user_id,
            (SELECT id FROM UNNEST(f.associated_ids) WHERE id_type = 3) AS visit_id,
        FROM
            `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            ON a.bucketing_id = f.bucketing_id
            AND a.bucketing_ts = f.event_ts
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
    );
END IF;


----posthoc analysis

-- 1a
SELECT
    event_id,
    a.variant_id,
    COUNT(*) AS total_units_in_variant,
    AVG(IF(COALESCE(event_value, 0) = 0, 0, 1)) AS percent_units_with_event,
    AVG(COALESCE(event_value, 0)) AS avg_events_per_unit,
    AVG(IF(COALESCE(event_value, 0) = 0, NULL, COALESCE(event_value, 0))) AS avg_events_per_unit_with_event
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
GROUP BY ALL
ORDER BY 1,2
;


-- 1b
WITH boe_recs_landing AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "boe_recs_landing"
)
, visual_search_tapped_backend AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "visual_search_tapped_backend"
)
SELECT
a.variant_id,
COUNT(DISTINCT a.bucketing_id) AS total_unit,
COUNT(DISTINCT CASE WHEN b.event_value IS NOT NULL THEN b.bucketing_id ELSE NULL END) AS total_unit_w_boe_recs_landing,
COUNT(DISTINCT CASE WHEN c.event_value IS NOT NULL THEN c.bucketing_id ELSE NULL END) AS total_unit_w_visual_search_tapped_backend
FROM `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
LEFT JOIN boe_recs_landing b
    ON a.bucketing_id = b.bucketing_id
LEFT JOIN visual_search_tapped_backend c
    ON a.bucketing_id = c.bucketing_id
GROUP BY ALL
;


WITH boe_recs_landing AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "boe_recs_landing"
)
, visual_search_tapped_backend AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "visual_search_tapped_backend"
)
SELECT
a.variant_id,
COUNT(DISTINCT a.bucketing_id) AS total_unit,
COUNT(DISTINCT CASE WHEN b.event_value IS NOT NULL THEN b.bucketing_id ELSE NULL END) AS total_unit_w_boe_recs_landing,
COUNT(DISTINCT CASE WHEN c.event_value IS NOT NULL THEN c.bucketing_id ELSE NULL END) AS total_unit_w_visual_search_tapped_backend
FROM `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
LEFT JOIN boe_recs_landing b
    ON a.bucketing_id = b.bucketing_id
LEFT JOIN visual_search_tapped_backend c
    ON a.bucketing_id = c.bucketing_id
GROUP BY ALL
;

WITH boe_recs_landing AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "boe_recs_landing"
)
, visual_search_tapped_backend AS (
SELECT
    event_id,
    a.variant_id,
    a.bucketing_id,
    CAST(event_value AS INT64) AS event_value
FROM
        `etsy-data-warehouse-dev.nlao.ab_first_bucket` a
    CROSS JOIN
        `etsy-data-warehouse-dev.nlao.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.nlao.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
WHERE event_id = "visual_search_tapped_backend"
)
SELECT
coalesce(b.variant_id,c.variant_id) AS variant_id,
CASE WHEN b.event_value IS NOT NULL THEN 1 ELSE 0 END AS boe_recs_landing,
CASE WHEN c.event_value IS NOT NULL THEN 1 ELSE 0 END AS visual_search_tapped_backend,
COUNT(*) AS ct
FROM boe_recs_landing b
FULL JOIN visual_search_tapped_backend c
    ON b.bucketing_id = c.bucketing_id
GROUP BY ALL
;

-- 1c
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.boe_recs_landing_by_app_version` AS (
SELECT 
sv.variant_id,
sv.bucketing_id,
sv.visit_id,
rv.user_agent,
SUM(CASE WHEN visual.event_type IS NOT NULL THEN 1 ELSE 0 END) AS visual_similar_event_ct,
SUM(CASE WHEN landing.event_type IS NOT NULL THEN 1 ELSE 0 END) AS boe_recs_landing_event_ct
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` sv
JOIN `etsy-data-warehouse-prod.weblog.recent_visits` rv
  ON sv.visit_id = rv.visit_id
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` visual
  ON sv.visit_id = visual.visit_id
   AND visual.event_type = "visual_search_tapped_backend"
LEFT JOIN `etsy-data-warehouse-prod.weblog.events` landing
  ON sv.visit_id = landing.visit_id
   AND landing.event_type = "boe_recs_landing"
WHERE rv._date >= '2025-05-30'
GROUP BY ALL
)
;

SELECT
case when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.29')) then '7.29'
     when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.28')) then '7.28'
     when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.27')) then '7.27'
     when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.26')) then '7.26'
     when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.25')) then '7.25'
     when REGEXP_CONTAINS(lower(user_agent), lower('EtsyInc/7.24')) then '7.24'
     else 'other' end AS app_version,
CASE WHEN visual_similar_event_ct >=1 THEN 1 ELSE 0 END AS visual_similar_event_ct,
CASE WHEN boe_recs_landing_event_ct >=1 THEN 1 ELSE 0 END AS boe_recs_landing_event_ct,
COUNT(*) AS visit_ct
FROM `etsy-data-warehouse-dev.nlao.boe_recs_landing_by_app_version`
WHERE variant_id = 'on'
GROUP BY ALL
;

-- 2a
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.ule_visually_similar_AA` AS (
  SELECT
    DISTINCT DATE(_partitiontime) AS _date
    , v.variant_id
    , b.visit_id
    , beacon.browser_id
    , beacon.user_id
    , beacon.event_name
    , CAST((SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_id") AS INT64) AS listing_id
    , (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_set_key") AS listing_set_key
    , (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "source") AS source
    , (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "placement") AS placement
  FROM
    `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
  JOIN `etsy-data-warehouse-dev.nlao.subsequent_visits` v
    ON b.visit_id = v.visit_id
  WHERE
    beacon.event_name IN ("listing_impression","listing_interaction")
    AND DATE(_partitiontime) BETWEEN '2025-05-30' AND '2025-06-12'
    AND 
    (
      (
        (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "source") = "recs" 
        AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "placement") = "boe_listing_screen_visually_similar"
      ) 
    OR
      (
        (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "source") = "ads" 
        AND (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "placement") = "avsg"
      ) 
    )
);

SELECT
variant_id,
placement,
sum(case when event_name = "listing_impression" then 1 else 0 end) as impression_ct,
sum(case when event_name = "listing_interaction" then 1 else 0 end) as click_ct,
FROM `etsy-data-warehouse-dev.nlao.ule_visually_similar_AA`
GROUP BY ALL
;

-- 3a
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.boe_recs_landing_boe_listing_screen_visually_similar_seen` AS (
SELECT 
    DISTINCT DATE(_partitiontime) AS _date,
    b.visit_id,
    beacon.event_name,
    b.sequence_number,
    b.beacon.event_timestamp,
    (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE KEY = "listing_ids") AS listing_ids,
    sv.variant_id,
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` sv
JOIN `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
      ON b.visit_id = sv.visit_id
WHERE DATE(_PARTITIONTIME) >= '2025-05-30'
AND beacon.event_name = 'recommendations_module_seen'
AND (select value from unnest(beacon.properties.key_value) where key = "module_placement") = "boe_listing_screen_visually_similar"
);

SELECT
variant_id,
count(*) AS recommendations_module_seen_ct
FROM `etsy-data-warehouse-dev.nlao.boe_recs_landing_boe_listing_screen_visually_similar_seen`
GROUP BY 1
;

