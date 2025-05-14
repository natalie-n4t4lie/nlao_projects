-- INPUT
DECLARE config_flag_param STRING DEFAULT "boe_home/app_home.feed.evergreen_frozen_pc";
DECLARE target_module_placement STRING DEFAULT "boe_homescreen_feed";
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

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.experiment_delivered_listings` AS (
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
r.added_to_cart,
r.purchased_after_view,
r.listing_rank,
r.sequence_number
FROM `etsy-data-warehouse-dev.nlao.subsequent_visits` v
JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
  ON v.visit_id = r.visit_id
WHERE module_placement = target_module_placement
AND _date BETWEEN '2025-04-16' AND '2025-04-27'
)
;

SELECT
variant_id,
candidate_set,
COUNT(*) AS delivered_listings,
SUM(seen) AS seen_listings,
SUM(clicked) AS clicked_listings,
SUM(added_to_cart) AS carted_listings,
SUM(purchased_after_view) AS purchased_listings
FROM `etsy-data-warehouse-dev.nlao.experiment_delivered_listings`
GROUP BY ALL
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.experiment_homefeed_listing_rank` AS (
with listing_rank as (
  select
  v.variant_id,
  v.visit_id,
  d.sequence_number,
  d.listing_id,
  d.candidate_set,
  d.seen,
  d.clicked,
  d.added_to_cart,
  d.purchased_after_view,
  d.listing_rank,
  row_number() over (partition by v.visit_id order by listing_rank) as rw
from `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` d
JOIN `etsy-data-warehouse-dev.nlao.subsequent_visits` v
  ON d.visit_id = v.visit_id
where d.module_placement = 'boe_homescreen_feed'
AND d._date BETWEEN '2025-04-16' AND '2025-04-27'
and d.seen = 1
), event_page_views as ( -- only look at page views per each visit_id that have a homescreen feed view
  select
  lr.variant_id,
  lr.visit_id,
  e.sequence_number,
  lr.candidate_set,
  event_type
  from `etsy-data-warehouse-prod.weblog.events` e
  inner join listing_rank lr using (visit_id)
  where page_view = 1
  and _date BETWEEN '2025-04-16' AND '2025-04-27'
), bookend_pages as ( -- cte to get the current page view and then the next page_view that comes afterwards
select
visit_id,
sequence_number as page_view,
event_type,
concat(event_type,sequence_number) as page_on,
coalesce(lead(sequence_number) over (partition by visit_id order by sequence_number),9999) as next_page_view --adding a coalesce statement with a large number here to cover for the last page a user visits
from event_page_views
), redone_listing_rank as ( -- rewrite listing rank to accomodate multiple feed refreshes in one page view
select
lr.*, page_view
from listing_rank lr
left join bookend_pages bp on lr.visit_id = bp.visit_id and lr.sequence_number > bp.page_view and lr.sequence_number < next_page_view
where page_view is not null
), finalized_product as (
  select
  variant_id,
  candidate_set,
visit_id,
sequence_number,
listing_id,
seen,
clicked,
added_to_cart,
purchased_after_view,
page_view,
row_number() over (partition by visit_id, page_view order by (sequence_number - page_view) * 100 + listing_rank) as listing_rank -- I add an large number to the difference between the sequence_number in recommendation module delivered event and the page_view event so each successive recommendation module delivered event gets a larger listing rank
from redone_listing_rank
),
with_in_visit_feed_view_number as (
select
  count(*),
  visit_id,
  page_view,
  row_number() over (partition by visit_id order by page_view) as in_visit_feed_view_number -- a counter to track multiple different views of the feed in a visit
from finalized_product
group by visit_id, page_view
),
final as (
select
finalized_product.visit_id,
variant_id,
candidate_set,
in_visit_feed_view_number,
listing_rank,
listing_id,
clicked,
seen,
added_to_cart,
purchased_after_view
from finalized_product
join with_in_visit_feed_view_number
  on with_in_visit_feed_view_number.visit_id = finalized_product.visit_id
  and with_in_visit_feed_view_number.page_view = finalized_product.page_view
)
SELECT
variant_id,
candidate_set,
final.visit_id,
in_visit_feed_view_number,
listing_rank,
listing_id,
clicked,
seen,
added_to_cart,
purchased_after_view
FROM final
);

--top 10 distribution
WITH cte AS (
SELECT
visit_id,
in_visit_feed_view_number,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" THEN 1 ELSE 0 END) AS popular_cluster_listing_ct,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" THEN 1 ELSE 0 END) AS evergreen_listing_ct,
SUM(clicked) AS click_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND clicked = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_click,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND clicked = 1 THEN 1 ELSE 0 END) AS evergreen_listing_click,
SUM(added_to_cart) AS cart_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_cart,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS evergreen_listing_cart,
SUM(purchased_after_view) AS purchase_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_purchase,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS evergreen_listing_purchase,
FROM `etsy-data-warehouse-dev.nlao.experiment_homefeed_listing_rank`
WHERE listing_rank<=10
AND variant_id = "on"
GROUP BY ALL
)
SELECT
popular_cluster_listing_ct,
evergreen_listing_ct,
COUNT(CONCAT(visit_id,in_visit_feed_view_number)) AS delivery_ct,
SUM(click_ct) AS click_ct,
SUM(popular_cluster_listing_click) AS popular_cluster_listing_click,
SUM(evergreen_listing_click) AS evergreen_listing_click,
SUM(cart_ct) AS cart_ct,
SUM(popular_cluster_listing_cart) AS popular_cluster_listing_cart,
SUM(evergreen_listing_cart) AS evergreen_listing_cart,
SUM(purchase_ct) AS purchase_ct,
SUM(popular_cluster_listing_purchase) AS popular_cluster_listing_purchase,
SUM(evergreen_listing_purchase) AS evergreen_listing_purchase
FROM cte
WHERE popular_cluster_listing_ct + evergreen_listing_ct = 10
GROUP BY ALL
ORDER BY 2,1
;

-- top 30
WITH cte AS (
SELECT
visit_id,
in_visit_feed_view_number,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" THEN 1 ELSE 0 END) AS popular_cluster_listing_ct,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" THEN 1 ELSE 0 END) AS evergreen_listing_ct,
SUM(clicked) AS click_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND clicked = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_click,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND clicked = 1 THEN 1 ELSE 0 END) AS evergreen_listing_click,
SUM(added_to_cart) AS cart_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_cart,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS evergreen_listing_cart,
SUM(purchased_after_view) AS purchase_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_purchase,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS evergreen_listing_purchase,
FROM `etsy-data-warehouse-dev.nlao.experiment_homefeed_listing_rank`
WHERE listing_rank<=30
AND variant_id = "on"
GROUP BY ALL
)
SELECT
popular_cluster_listing_ct,
evergreen_listing_ct,
COUNT(CONCAT(visit_id,in_visit_feed_view_number)) AS delivery_ct,
SUM(click_ct) AS click_ct,
SUM(popular_cluster_listing_click) AS popular_cluster_listing_click,
SUM(evergreen_listing_click) AS evergreen_listing_click,
SUM(cart_ct) AS cart_ct,
SUM(popular_cluster_listing_cart) AS popular_cluster_listing_cart,
SUM(evergreen_listing_cart) AS evergreen_listing_cart,
SUM(purchase_ct) AS purchase_ct,
SUM(popular_cluster_listing_purchase) AS popular_cluster_listing_purchase,
SUM(evergreen_listing_purchase) AS evergreen_listing_purchase
FROM cte
WHERE popular_cluster_listing_ct + evergreen_listing_ct = 30
GROUP BY ALL
ORDER BY 2,1
;

-- top 50
WITH cte AS (
SELECT
visit_id,
in_visit_feed_view_number,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" THEN 1 ELSE 0 END) AS popular_cluster_listing_ct,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" THEN 1 ELSE 0 END) AS evergreen_listing_ct,
SUM(clicked) AS click_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND clicked = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_click,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND clicked = 1 THEN 1 ELSE 0 END) AS evergreen_listing_click,
SUM(added_to_cart) AS cart_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_cart,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND added_to_cart = 1 THEN 1 ELSE 0 END) AS evergreen_listing_cart,
SUM(purchased_after_view) AS purchase_ct,
SUM(CASE WHEN candidate_set = "arizona:Evergreen_Clusters_Listing_V1" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS popular_cluster_listing_purchase,
SUM(CASE WHEN candidate_set = "arizona:EVERGREEN_INTERESTS_HEURISTICS_WITH_XWALK_FEED" AND purchased_after_view = 1 THEN 1 ELSE 0 END) AS evergreen_listing_purchase,
FROM `etsy-data-warehouse-dev.nlao.experiment_homefeed_listing_rank`
WHERE listing_rank<=50
AND variant_id = "on"
GROUP BY ALL
)
SELECT
popular_cluster_listing_ct,
evergreen_listing_ct,
COUNT(CONCAT(visit_id,in_visit_feed_view_number)) AS delivery_ct,
SUM(click_ct) AS click_ct,
SUM(popular_cluster_listing_click) AS popular_cluster_listing_click,
SUM(evergreen_listing_click) AS evergreen_listing_click,
SUM(cart_ct) AS cart_ct,
SUM(popular_cluster_listing_cart) AS popular_cluster_listing_cart,
SUM(evergreen_listing_cart) AS evergreen_listing_cart,
SUM(purchase_ct) AS purchase_ct,
SUM(popular_cluster_listing_purchase) AS popular_cluster_listing_purchase,
SUM(evergreen_listing_purchase) AS evergreen_listing_purchase
FROM cte
WHERE popular_cluster_listing_ct + evergreen_listing_ct = 50
GROUP BY ALL
ORDER BY 2,1
;

-- engagement by scroll depth
WITH cte AS (
SELECT
variant_id,
visit_id,
in_visit_feed_view_number,
MAX(listing_rank) AS deepest_listing_seen,
MAX(clicked) AS clicked
FROM `etsy-data-warehouse-dev.nlao.experiment_homefeed_listing_rank`
GROUP BY ALL
)
SELECT
variant_id,
CASE WHEN deepest_listing_seen<=30 THEN "1"
     WHEN deepest_listing_seen<=60 THEN "2"
     WHEN deepest_listing_seen<=90 THEN "3"
     WHEN deepest_listing_seen<=120 THEN "4"
     WHEN deepest_listing_seen<=150 THEN "5"
     WHEN deepest_listing_seen>150 THEN "5+"
END AS deepest_listing_seen,
SUM(clicked) AS click_count,
COUNT(visit_id) AS visit_ct,
SUM(clicked)/ COUNT(visit_id) AS ctr
FROM cte
GROUP BY ALL
;

-- taxo diversity
WITH cte AS (
SELECT DISTINCT
variant_id,
visit_id,
-- candidate_set,
COUNT(*) AS delivered_listings,
COUNT(distinct rec_taxonomy_id) AS taxonomy_id_ct
FROM `etsy-data-warehouse-dev.nlao.experiment_delivered_listings`
WHERE seen = 1
GROUP BY ALL
)
SELECT
variant_id,
-- candidate_set,
AVG(taxonomy_id_ct) AS avg_taxo_per_user
FROM cte
GROUP BY ALL
;



