-------------------------------------------------------------------------------------------
-- INPUT
-------------------------------------------------------------------------------------------
DECLARE config_flag_param STRING DEFAULT "mobile_dynamic_config.iphone.BrowseyListingPreviewCTAArrangement";

-- By default, this script uses the latest experiment boundary dates for the given experiment.
-- If you want to specify a custom date range, you can also specify the start and end date manually.
DECLARE start_date DATE; -- DEFAULT "2023-08-22";
DECLARE end_date DATE; -- DEFAULT "2023-09-04";

-- By default, this script automatically detects whether the experiment is event filtered or not
-- and provides the associated analysis. However, in the case that we want to examine non-filtered
-- results for an event filtered experiment, this variable may be manually set to "FALSE".
DECLARE is_event_filtered BOOL; -- DEFAULT FALSE;

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
--   - Replace 'ldap' in the table names below with your own username or personal dataset name.
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.first_bucket_segments` AS (
    SELECT
        *
    FROM
        `etsy-data-warehouse-dev.nlao.first_bucket_segments_unpivoted`
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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.events` AS (
    SELECT
        *
    FROM
        UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "prolist_total_spend",  -- prolist revenue
            "gms",                  -- note: gms data is in cents
            "listing_preview_viewed"
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

SELECT
*
FROM `etsy-data-warehouse-dev.nlao.events_per_unit`
WHERE event_id = "listing_preview_viewed"
AND CAST(event_value AS INT64) >1
limit 10
;


-- create a temp table of all the buyers eligible for research
with can_be_contacted_feedback as (
  select 
    ui.user_id,
    ui.primary_email as Email -- qualtrics requires email column to be Email
  from 
    `etsy-data-warehouse-prod.etsy_index.users_index` ui
  where
    ui.is_seller = 0 -- they are not a seller
    and ui.is_admin = 0 -- who is not an etsy admin
    and ui.is_nipsa = 0 -- and aren't in trouble with etsy
    and ui.is_frozen = 0 -- or frozen
    and ui.user_state = 'active' -- and they're active
    -- and they haven't been contacted in 6 months for research:
    and ui.user_id not in (
      select distinct 
        user_id
      from 
        `etsy-data-warehouse-prod.research.ux_participants`
      where 
        -- can change this to 3 month if need more sample, 
        -- but check the dashboard first
        date(date_contacted) >= date_sub(current_date,interval 6 month)
    )
    -- who have agreed to give us feedback:
  	and ui.user_id in (
  	  select distinct 
  	      user_id
      from 
          `etsy-data-warehouse-prod.rollups.email_subscribers`
      where
        campaign_label = 'feedback'
        and campaign_state = 'subscribed'
        and verification_state = 'confirmed'
    )
    -- and who haven't opted out of emails:    
    and ui.user_id not in (
      select distinct 
        user_id
      from 
        `etsy-data-warehouse-prod.etsy_shard.user_preferences`
      where 
        preference_id = 704 
        and preference_setting = 'false'
    )
),
-- support info - open cases
open_case_count as (
  select
    buyer_user_id as user_id,
    sum(
      case
        when closed_date is null then 1
        else 0
      end
    ) as n_open_cases
  from
    etsy-data-warehouse-prod.rollups.user_cases
  group by
    1
),
-- support info - tickets in past 90 days
n_recent_cases as (
  select
    buyer_user_id as user_id,
    count(*) as n_cases_90
  from
    etsy-data-warehouse-prod.rollups.user_cases
  where 
    date(timestamp_seconds(case_date)) >= 
      date_sub(current_date, interval 90 day)  
  group by
    1
),

-- top operating system for visits 

visits_by_os as (
  select
    user_id,
    sum(desktop_visits) as desk,
    sum(ios_os_visits) as ios,
    sum(android_os_visits) as android,
    sum(other_os_visits) as other,
    sum(undefined_os_visits) as undef
  from
    etsy-data-warehouse-prod.user_mart.user_visit_ltd
  group by
    1
),
visit_os as (
  select
    user_id,
    case
      when greatest(desk, ios, android, other, undef) = desk then 'desktop'
      when greatest(desk, ios, android, other, undef) = ios then 'ios'
      when greatest(desk, ios, android, other, undef) = android then 'android'
      else 'other/undefined'
    end as top_visit_os
  from
    visits_by_os
)
, experiment_audience AS (
    SELECT
        e.*,
        v.user_id
    FROM `etsy-data-warehouse-dev.nlao.events_per_unit` e
    JOIN `etsy-data-warehouse-prod.weblog.recent_visits` v
        ON e.bucketing_id = v.browser_id 
    WHERE event_id = "listing_preview_viewed"
        AND CAST(event_value AS INT64) > 1 -- having more than 1 blp
        AND v._date >= '2025-03-31'
)

select
-- from buyer_basics
  a.user_id,
  a.Email,
  b.buyer_segment,
  b.us_state, -- US only --
  b.country,
  b.first_purchase_date, --defined as first purchase --
  b.primary_platform_app,
  b.lifetime_orders,
  b.past_year_orders,
  b.lifetime_top_category,
  b.lifetime_top_subcategory,
  b.last_purchase_date,
  date(timestamp_seconds(b.last_visit_date)) as last_visit_date_clean,
  ups.top_purch_device, -- top device for purchases
  uvs.top_visit_device, -- top device for visits
  vo.top_visit_os, -- top operating system for visits
  -- add other variables from buyer basics with b.variable_name, as necessary
  ea.variant_id,
  ea.event_value AS blp_event_count
from
  can_be_contacted_feedback a
left join 
  `etsy-data-warehouse-prod.rollups.buyer_basics` b 
    on a.user_id = b.mapped_user_id
left join
  open_case_count occ on a.user_id = occ.user_id
left join
  n_recent_cases nrc on a.user_id = nrc.user_id
left join
  `etsy-data-warehouse-prod.user_mart.user_purch_stats` ups
    on a.user_id = ups.user_id
left join
  `etsy-data-warehouse-prod.user_mart.user_visit_stats` uvs
    on a.user_id = uvs.user_id
left join
  visit_os vo on a.user_id = vo.user_id
join 
  experiment_audience ea on a.user_id = ea.user_id
where
    b.country = 'United States' -- for US-only results
AND b.buyer_segment in ('Active', 'Habitual', 'High Potential', 'Repeat', 'OTB') ---- update the segments you want; remove active and OTB for just "targets"
AND date(timestamp_seconds(last_visit_date)) > date_sub(current_date, interval 6 month) --specifies buyers that have visited in last 6 months
AND vo.top_visit_os in ('desktop','ios','android') --top visit OS
AND b.primary_platform_app in ('desktop','boe','mobile_web') --top platform
AND (occ.n_open_cases is null or occ.n_open_cases = 0) --no open cases
AND (nrc.n_cases_90 is null or nrc.n_cases_90 = 0) --no support tickets in last 90 days
  -- add other where clauses as necessary
  -- see go/schemer and search buyer_basics all the variables available
order by 
  rand()
limit 
  -- update the limit based on your sample needs and estimated response rate
  5000
;

