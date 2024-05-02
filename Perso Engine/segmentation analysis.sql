/* SOURCE: https://github.etsycorp.com/Engineering/AnalystWork/blob/master/ExperimentResources/catapult_unified_event_level_data_simplified.sql
Note:
This script contains queries on the catapult_unified tables that can be used for analyzing experiment results.
It takes in an experiment (config flag) and target module placement (for recommendation module clicked/seen segmentation) as input, and provides the following data:
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
DECLARE config_flag_param STRING DEFAULT "perso_engine.recs.compl_clip_v2_boe_ty";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify an earlier experiment boundary, you can do so by specifying the start and end date manually.
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";

-- By default, this script automatically detects whether the experiment is event filtered or not
-- and provides the associated analysis. However, in the case that we want to examine non-filtered
-- results for an event filtered experiment, this variable may be manually set to "FALSE".
DECLARE is_event_filtered BOOL DEFAULT FALSE;

-- Generally, this variable should not be overridden, as the grain of analysis should match the
-- bucketing ID type.
DECLARE bucketing_id_type INT64;

-- DEFINE target module_placement here
DECLARE target_module_placement STRING DEFAULT "boe_homescreen_post_purchase_people_also_bought";

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
--   - Replace 'ldap' in the table names below with your own username or personal dataset name.
--   - Additionally, there are a few TODO items in this script depending on:
--       - Whether you would like to look at certain segmentations  (marked with <SEGMENTATION>)
--       - Whether you would like to look at certain events         (marked with <EVENT>)
--     Before running, please review the script and adjust the marked sections accordingly!

-------------------------------------------------------------------------------------------
-- BUCKETING DATA
-------------------------------------------------------------------------------------------
-- Get the first bucketing moment for each experimental unit (e.g. browser or user).
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.ab_first_bucket` AS (
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
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.ab_first_bucket` AS (
        SELECT
            a.bucketing_id,
            a.bucketing_id_type,
            a.variant_id,
            MIN(f.event_ts) AS bucketing_ts,
        FROM
            `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.first_bucket_segments_unpivoted` AS (
    SELECT
        a.bucketing_id,
        a.variant_id,
        s.event_id,
        s.event_value,
    FROM
        `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
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
            "canonical_region"
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.first_bucket_segments` AS (
    SELECT
        *
    FROM
        `etsy-data-warehouse-dev.ldap.first_bucket_segments_unpivoted`
    PIVOT(
        MAX(event_value)
        FOR event_id IN (
            "buyer_segment",
            "canonical_region"
        )
    )
);

-------------------------------------------------------------------------------------------
-- EVENT AND GMS DATA
-------------------------------------------------------------------------------------------
-- <EVENT> Specify the events you want to analyze here.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.events` AS (
    SELECT
        *
    FROM
        UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "prolist_total_spend",  -- prolist revenue
            "gms"                   -- note: gms data is in cents
        ]) AS event_id
);

-- Get all the bucketed units with the events of interest.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.events_per_unit` AS (
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
        `etsy-data-warehouse-dev.ldap.events`
        USING(event_id)
    JOIN
        `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
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
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.post_bucketing_custom_events` AS (
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
                `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
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

    INSERT INTO `etsy-data-warehouse-dev.ldap.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.ldap.post_bucketing_custom_events`
        WHERE
            row_number = 1
            OR (row_number > 1 AND sequence_number = 0)
        GROUP BY
            bucketing_id, variant_id, event_id
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.post_bucketing_custom_events` AS (
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
                `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
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

    INSERT INTO `etsy-data-warehouse-dev.ldap.events_per_unit` (
        SELECT
            bucketing_id,
            variant_id,
            event_id,
            SUM(event_value) AS event_value,
        FROM
            `etsy-data-warehouse-dev.ldap.post_bucketing_custom_events`
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
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
            v.converted
        FROM
            `etsy-data-warehouse-dev.ldap.ab_first_bucket` b
        JOIN
            `etsy-data-warehouse-prod.weblog.visits` v
            ON b.bucketing_id = v.browser_id
            AND TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        WHERE
            v._date BETWEEN start_date AND end_date
    );
ELSEIF bucketing_id_type = 2 THEN -- user data (see go/catapult-unified-enums)
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.subsequent_visits` AS (
        SELECT
            b.bucketing_id,
            b.variant_id,
            v.visit_id,
            v.converted
        FROM
            `etsy-data-warehouse-dev.ldap.ab_first_bucket` b
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.visits_per_unit` AS (
    SELECT
        bucketing_id,
        variant_id,
        COUNT(*) AS visit_count,
        MAX(converted) AS converting_unit
    FROM
        `etsy-data-warehouse-dev.ldap.subsequent_visits`
    GROUP BY
        bucketing_id, variant_id
);

-------------------------------------------------------------------------------------------
-- COMBINE BUCKETING, EVENT & SEGMENT DATA
-------------------------------------------------------------------------------------------
-- All events for all bucketed units, with segment values.
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.all_units_events_segments` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        COALESCE(event_value, 0) AS event_count,
        IF(COALESCE(event_value, 0)=0, 0, 1) AS has_event,
        buyer_segment,
        canonical_region,
        converting_unit
    FROM
        `etsy-data-warehouse-dev.ldap.ab_first_bucket`
    CROSS JOIN
        `etsy-data-warehouse-dev.ldap.events`
    LEFT JOIN
        `etsy-data-warehouse-dev.ldap.events_per_unit`
        USING(bucketing_id, variant_id, event_id)
    JOIN
        `etsy-data-warehouse-dev.ldap.first_bucket_segments`
        USING(bucketing_id, variant_id)
    JOIN 
        `etsy-data-warehouse-dev.ldap.visits_per_unit`
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
    `etsy-data-warehouse-dev.ldap.all_units_events_segments`
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
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.ab_first_bucket` AS (
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
            `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.bucketing` b
            USING(bucketing_id, variant_id, bucketing_ts)
        WHERE
            b._date BETWEEN start_date AND end_date
            AND b.experiment_id = config_flag_param
    );
ELSE
    CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.ab_first_bucket` AS (
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
            `etsy-data-warehouse-dev.ldap.ab_first_bucket` a
        JOIN
            `etsy-data-warehouse-prod.catapult_unified.filtering_event` f
            ON a.bucketing_id = f.bucketing_id
            AND a.bucketing_ts = f.event_ts
        WHERE
            f._date BETWEEN start_date AND end_date
            AND f.experiment_id = config_flag_param
    );
END IF;

--========================================================================================--
------------------------------- DEFINE CUSTOM RECS SEGMENTS -------------------------------
--========================================================================================--
-- Included segments:
    -- 1. Target module Seen* (module_engagement_seen)
    -- 2. Target module Clicked* (module_engagement_clicked)
    -- 3. Target listing top category (target_top_category)
    -- 4. Target listing second category (target_listing_second_cat)
    -- 5. Candidate listing top category (rec_top_category)
    -- 6. Candidate listing second category (recs_listing_second_cat)
    -- 7. Target Price Range (target_price_range)
    -- 8. Candidate Price Range (rec_price_range)
    -- 9. Target and Candidate Listing Price Diff (target_rec_price_diff)
    -- 10.Target and Candidate Top Category Diff (target_rec_same_top_cat)
    -- 11.Target and Candidate Second Level Category Diff (target_rec_same_second_cat)
---------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap.recs_types` AS (
WITH module_engage_segments AS (
SELECT
    v.bucketing_id,
    v.variant_id,
    MAX(r.seen) AS seen,
    MAX(r.clicked) AS clicked
FROM `etsy-data-warehouse-dev.ldap.subsequent_visits` v
LEFT JOIN 
    (SELECT * 
    FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
    -- WHERE module_placement = target_module_placement
    WHERE module_placement = "boe_homescreen_post_purchase_people_also_bought"
    ) r
  ON v.visit_id = r.visit_id
GROUP BY 1,2  
)
SELECT DISTINCT
    v.bucketing_id,
    v.variant_id,
    CASE WHEN e.clicked = 1 OR e.seen = 1 THEN "1" ELSE "0" END AS module_engagement_seen,
    CASE WHEN e.clicked IS NULL THEN "0" ELSE CAST(e.clicked AS STRING) END AS module_engagement_clicked,
    r.target_top_category,
    CASE WHEN SPLIT(t1.full_path, ".")[SAFE_OFFSET(1)] IS NULL THEN "No recs listing second cat" 
    ELSE CONCAT(SPLIT(t1.full_path, ".")[SAFE_OFFSET(0)],'.', SPLIT(t1.full_path, ".")[SAFE_OFFSET(1)])
    END AS target_listing_second_cat, 
    r.rec_top_category,
    CASE WHEN SPLIT(t2.full_path, ".")[SAFE_OFFSET(1)] IS NULL THEN "No recs listing second cat" 
    ELSE CONCAT(SPLIT(t2.full_path, ".")[SAFE_OFFSET(0)],'.', SPLIT(t2.full_path, ".")[SAFE_OFFSET(1)])
    END AS recs_listing_second_cat, 
    CASE WHEN target_top_category IS NULL THEN "No target listing top cat"
         WHEN rec_top_category IS NULL THEN "No recs listing top_cat"
         WHEN target_top_category = rec_top_category THEN "1" 
         ELSE "0" END AS target_rec_same_top_cat,
    CASE WHEN SPLIT(t1.full_path, ".")[SAFE_OFFSET(1)] IS NULL THEN "No target listing second cat"
         WHEN SPLIT(t2.full_path, ".")[SAFE_OFFSET(1)] IS NULL THEN "No recs listing second cat"
         WHEN SPLIT(t1.full_path, ".")[SAFE_OFFSET(1)] = SPLIT(t2.full_path, ".")[SAFE_OFFSET(1)] THEN "1" 
         ELSE "0" END AS target_rec_same_second_cat,
    CASE WHEN r.target_price IS NULL THEN "0. No target listing price"
         WHEN r.target_price <=50 THEN "1. $0-$50"
         WHEN r.target_price <=100 THEN "2. $50-$100"
         WHEN r.target_price <=500 THEN "3. $100-$500"
         WHEN r.target_price >500 THEN "4. $500+"
         END AS target_price_range,
    CASE WHEN r.rec_price IS NULL THEN "0. No recs listing price"
         WHEN r.rec_price <=50 THEN "1.$0-$50"
         WHEN r.rec_price <=100 THEN "2.$50-$100"
         WHEN r.rec_price <=500 THEN "3.$100-$500"
         WHEN r.rec_price >500 THEN "4.$500+"   
         END AS rec_price_range,
    CASE WHEN r.target_price IS NULL OR r.rec_price IS NULL THEN NULL
         WHEN r.rec_price/ r.target_price - 1 < -1 THEN "1. Recs 100%+ cheaper"
         WHEN r.rec_price/ r.target_price - 1 < -0.5 AND r.rec_price/ r.target_price - 1 >= -1 THEN "2. Recs 50% - 100% cheaper"
         WHEN r.rec_price/ r.target_price - 1 < -0.25 AND r.rec_price/ r.target_price - 1 >= -0.5 THEN "3. Recs 25% - 50% cheaper"
         WHEN r.rec_price/ r.target_price - 1 < -0.1 AND r.rec_price/ r.target_price - 1 >= -0.25 THEN "4. Recs 10% - 25% cheaper"
         WHEN r.rec_price/ r.target_price - 1 < 0 AND r.rec_price/ r.target_price - 1 >= -0.1 THEN "5. Recs <= 10% cheaper"
         WHEN r.target_price = rec_price THEN "6. Same price" 
         WHEN r.rec_price/ r.target_price - 1 > 0 AND r.rec_price/ r.target_price - 1 <= 0.1 THEN "7. Recs <= 10% more expensive"
         WHEN r.rec_price/ r.target_price - 1 > 0.1 AND r.rec_price/ r.target_price - 1 <= 0.25 THEN "8. Recs 10% - 25% more expensive"
         WHEN r.rec_price/ r.target_price - 1 > 0.25 AND r.rec_price/ r.target_price - 1 <= 0.5 THEN "9. Recs 25% - 50% more expensive"
         WHEN r.rec_price/ r.target_price - 1 > 0.5 AND r.rec_price/ r.target_price - 1 <= 1 THEN "10. Recs 50% - 100% more expensive"
         WHEN r.rec_price/ r.target_price - 1 > 1 THEN "11. Recs 100%+ more expensive"
         END AS target_rec_price_diff
  FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
  JOIN `etsy-data-warehouse-dev.ldap`.subsequent_visits v
    ON r.visit_id = v.visit_id  -- TO SEGMENT BY ALL VISITS (allows one browser to fall in multiple segments)
  JOIN module_engage_segments e
    ON v.bucketing_id = e.bucketing_id
  LEFT JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t1
    ON t1.taxonomy_id = r.target_taxonomy_id
  LEFT JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t2
    ON t2.taxonomy_id = r.rec_taxonomy_id
  WHERE r._date BETWEEN start_date AND end_date -- experiment dates
)
;

--========================================================================================--
-------------------------------- SEGEMENT RESULT TABLE -------------------------------------
--========================================================================================--

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ldap`.browser_event_data_recs_segments AS (
-- 1.Target module Seen* (module_engagement_seen)
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Module Engagement Seen" AS segment_type,
    module_engagement_seen AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss  
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 2.Target module Clicked* (module_engagement_clicked)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Module Engagement Clicked" AS segment_type,
    module_engagement_clicked AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss   
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 3.Target listing top category (target_top_category)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target listing top category" AS segment_type,
    target_top_category AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 4.Target listing second category (target_listing_second_cat)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target listing second category" AS segment_type,
    target_listing_second_cat AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 5.Candidate listing top category (rec_top_category)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Candidate listing top category" AS segment_type,
    rec_top_category AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 6.Candidate listing second category (recs_listing_second_cat)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Candidate listing second category" AS segment_type,
    recs_listing_second_cat AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 7.Target Price Range (target_price_range)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target Price Range" AS segment_type,
    target_price_range AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 8.Candidate Price Range (rec_price_range)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Candidate Price Range" AS segment_type,
    rec_price_range AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss  
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 9.Target and Candidate Listing Price Diff (target_rec_price_diff)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target and Candidate Listing Price Diff" AS segment_type,
    target_rec_price_diff AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 10.Target and Candidate Top Category Diff (target_rec_same_top_cat)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target and Candidate Top Category Diff" AS segment_type,
    target_rec_same_top_cat AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss  
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
-- 11.Target and Candidate Second Category Diff (target_rec_same_second_cat)
UNION ALL
  SELECT
    config_flag_param AS experiment_id,
    a.variant_id AS ab_variant,
    a.event_id AS event_name,
    "Target and Candidate Second Category Diff" AS segment_type,
    target_rec_same_second_cat AS segment,
    COUNT(*) AS total_browsers_in_variant,
    COUNT(CASE WHEN a.converting_unit = 1 THEN a.bucketing_id END) AS total_converting_browsers_in_variant, -- new denominator for ACBV
    AVG(a.has_event) AS pct_browsers_with_event,
    SUM(a.has_event) AS browsers_with_event,
    AVG(a.event_count) AS avg_events_per_browser,
    AVG(CASE WHEN a.event_count > 0 THEN a.event_count END) AS avg_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN a.event_count END) AS sum_events_per_browser_with_event,
    SUM(CASE WHEN a.event_count > 0 THEN POWER(a.event_count,2) END) AS sum_events_per_browser_with_event_ss 
  FROM `etsy-data-warehouse-dev.ldap.all_units_events_segments` a
    JOIN `etsy-data-warehouse-dev.ldap.recs_types` b ON a.bucketing_id = b.bucketing_id
  GROUP BY 1, 2, 3, 4, 5
)
;
