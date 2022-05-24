##CREATE KEYWORD MATCHED LISTING##
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.sustainability_term_listings` AS
WITH
    terms AS(
        SELECT *
        FROM `etsy-data-warehouse-dev.nlao.sustainability_term`
        WHERE term NOT IN ('green')
    ),
    active_listing_desc AS(
        SELECT 
            a.listing_id,
            a.description,
            a.title
        FROM `etsy-data-warehouse-prod.etsy_shard.listings` a
        JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` b
            ON a.listing_id = b.listing_id
        WHERE a.description IS NOT NULL
    ),
    searchable_desc_and_titles AS (
        SELECT
            listing_id,
            ' '|| LOWER(REGEXP_REPLACE(description, '[^A-Za-z0-9-]', ' '))|| ' ' AS description_searchable,
            ' '|| LOWER(REGEXP_REPLACE(title, '[^A-Za-z0-9-]', ' '))|| ' ' AS title_searchable
        FROM active_listing_desc
    ),
    listings_WITH_term_in_desc AS (
        SELECT
            d.listing_id,
            MAX(CASE WHEN category = 'sustainable material' THEN 1 ELSE 0 END) AS contains_sustainable_material_term,
            MAX(CASE WHEN category = 'generic eco labels' THEN 1 ELSE 0 END) AS contains_generic_eco_labels_term,
            MAX(CASE WHEN category = 'sustainable lifestyle' THEN 1 ELSE 0 END) AS contains_sustainable_lifestyle_term,
            MAX(CASE WHEN category = 'preferred manufacturing' THEN 1 ELSE 0 END) AS contains_preferred_manufacturing_term,
            MAX(CASE WHEN category = 'misc sustainability' THEN 1 ELSE 0 END) AS contains_misc_sustainability_term
        FROM searchable_desc_and_titles d
        JOIN terms t
            ON REGEXP_CONTAINS(d.description_searchable, ' '|| t.term || ' ')
        GROUP BY 1
    ),
    listings_with_term_in_title AS (
        SELECT
            d.listing_id,
            MAX(CASE WHEN category = 'sustainable material' THEN 1 ELSE 0 END) AS contains_sustainable_material_term,
            MAX(CASE WHEN category = 'generic eco labels' THEN 1 ELSE 0 END) AS contains_generic_eco_labels_term,
            MAX(CASE WHEN category = 'sustainable lifestyle' THEN 1 ELSE 0 END) AS contains_sustainable_lifestyle_term,
            MAX(CASE WHEN category = 'preferred manufacturing' THEN 1 ELSE 0 END) AS contains_preferred_manufacturing_term,
            MAX(CASE WHEN category = 'misc sustainability' THEN 1 ELSE 0 END) AS contains_misc_sustainability_term
        FROM searchable_desc_and_titles d
        JOIN terms t
            ON REGEXP_CONTAINS(d.title_searchable, ' '|| t.term || ' ')  
        GROUP BY 1     
    )
    SELECT 
        coalesce(a.listing_id, b.listing_id) AS listing_id,
        CASE WHEN a.listing_id IS NOT NULL THEN 1 ELSE 0 END AS term_in_desc,
        CASE WHEN b.listing_id IS NOT NULL THEN 1 ELSE 0 END AS term_in_title,
        coalesce(a.contains_sustainable_material_term, b.contains_sustainable_material_term, 0)  AS contains_sustainable_material_term,
        coalesce(a.contains_generic_eco_labels_term, b.contains_generic_eco_labels_term, 0) AS contains_generic_eco_labels_term,
        coalesce(a.contains_sustainable_lifestyle_term, b.contains_sustainable_lifestyle_term, 0) AS contains_sustainable_lifestyle_term,
        coalesce(a.contains_preferred_manufacturing_term, b.contains_preferred_manufacturing_term, 0) AS contains_preferred_manufacturing_term,
        coalesce(a.contains_misc_sustainability_term, b.contains_misc_sustainability_term, 0) AS ontains_misc_sustainability_term
    FROM listings_with_term_in_desc a
    FULL OUTER JOIN listings_with_term_in_title b
        ON a.listing_id = b.listing_id
;
## LISTING COVERAGE ##
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(*) AS count_total_active_listings,
          COUNT(DISTINCT shop_id) AS count_total_active_shop
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(contains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*,
    count_listings_sustainability_term_in_desc / count_total_sustainability_listings AS count_listings_sustainability_term_in_desc_pct_of_sustainability_listings,
    count_listings_sustainability_term_in_title / count_total_sustainability_listings AS count_listings_sustainability_term_in_title_pct_of_sustainability_listings,
    count_listings_sustainability_term_in_both_desc_and_title / count_total_sustainability_listings  AS count_listings_sustainability_term_in_both_desc_and_title_pct_of_sustainability_listings,
    count_listings_contains_generic_eco_labels_term / count_total_sustainability_listings AS count_listings_contains_generic_eco_labels_term_pct_of_sustainability_listings,
    count_listings_contains_misc_sustainability_term  / count_total_sustainability_listings AS count_listings_contains_misc_sustainability_term_pct_of_sustainability_listings,
    count_listings_contains_preferred_manufacturing_term / count_total_sustainability_listings AS count_listings_contains_preferred_manufacturing_term_pct_of_sustainability_listings,
    count_listings_contains_sustainable_lifestyle_term / count_total_sustainability_listings AS count_listings_contains_sustainable_lifestyle_term_pct_of_sustainability_listings,
    count_listings_contains_sustainable_material_term / count_total_sustainability_listings AS count_listings_contains_sustainable_material_term_pct_of_sustainability_listings
FROM susti_listing_counts a
CROSS JOIN active_listing_counts b
;

## Sustainability Term Listing Counts by Term ##
WITH
    terms AS (
        SELECT *
        FROM `etsy-data-warehouse-dev.nlao.sustainability_term`
        WHERE term NOT IN ('green')
    ),
    active_listing_desc AS(
        SELECT 
            a.listing_id,
            a.description,
            a.title
        FROM `etsy-data-warehouse-prod.etsy_shard.listings` a
        JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` b
            ON a.listing_id = b.listing_id
        WHERE a.description IS NOT NULL
    ),
    searchable_desc_and_titles AS(
        SELECT
            listing_id,
            ' '|| LOWER(REGEXP_REPLACE(description, '[^A-Za-z0-9-]', ' '))|| ' ' AS description_searchable,
            ' '|| LOWER(REGEXP_REPLACE(title, '[^A-Za-z0-9-]', ' '))|| ' ' AS title_searchable
        FROM active_listing_desc
    ),
    listings_with_term_in_desc AS(
        SELECT DISTINCT 
            d.listing_id,
            t.term,
            'description' AS location
        FROM searchable_desc_and_titles d
        JOIN terms t
            ON REGEXP_CONTAINS(d.description_searchable, ' '|| t.term || ' ')
    ),
    listings_with_term_in_title AS(
        SELECT DISTINCT
            d.listing_id,
            t.term,
            'title' AS location
        FROM searchable_desc_and_titles d
        JOIN terms t
            ON REGEXP_CONTAINS(d.title_searchable, ' '|| t.term || ' ')  
    ),
    combined AS(
        SELECT *
        FROM listings_with_term_in_title a
        UNION ALL 
        SELECT *
        FROM listings_with_term_in_title b
    )
SELECT term, COUNT(DISTINCT listing_id) count_listings
FROM combined 
GROUP BY 1
ORDER BY 2 desc
;




-- bucket events: list of events, any of which would trigger browser's inclusion in the experiment
declare bucket_events array<string> default ['listing view on sustainable listing'];
-- target events: list of events for which you want to see the base rate
declare target_events array<string> default ['backend_cart_payment'];
-- somewhat arbitrary start and end dates, but good idea to look over a recent time period of at least a week
declare platforms array<string> default ['mobile_web', 'desktop'];
declare start_date date default '2022-04-23';
declare end_date date default '2022-05-23';

with
  -- query listings eligible for experiment
  bucket_listings as (
      select listing_id
      from `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
  ),
  listing_counts as (
      select count(*) count_eligible_listings
      from bucket_listings
  ),
  bucket_events as (
    select
      v.browser_id,
      a._date,
      a.visit_id,
      sequence_number,
      epoch_ms as bucket_epoch_ms,
      row_number() over(partition by v.browser_id order by a._date, a.visit_id, sequence_number) rank
    from `etsy-data-warehouse-prod.analytics.listing_views` a
    join bucket_listings c
        on a.listing_id = c.listing_id
    join etsy-data-warehouse-prod.weblog.recent_visits v
      on a.visit_id = v.visit_id
    where
      a._date between start_date and end_date
      and v._date between start_date and end_date
      and a.platform in unnest(platforms)
  ),
  exp_browsers as (
    select browser_id, _date as bucket_date, visit_id as bucket_visit_id, sequence_number as bucket_seq, bucket_epoch_ms
    from bucket_events
    where rank = 1
  ),
  target_browsers as (
    select distinct b.browser_id, bucket_visit_id, bucket_epoch_ms 
    from etsy-data-warehouse-prod.weblog.events e
    join exp_browsers b
      on split(visit_id, '.')[offset(0)] = b.browser_id
      and (
        (e.visit_id = b.bucket_visit_id and e.sequence_number > b.bucket_seq)
        or
        (e.visit_id > b.bucket_visit_id)
       )
    join etsy-data-warehouse-prod.weblog.recent_visits v
      on e.visit_id = v.visit_id
    where
      e._date between start_date and end_date
      and v._date between start_date and end_date
      and event_type in unnest(target_events)
      and v.platform in unnest(platforms)
    ),
    gms_exp_browsers as (
        select sum(t.gms_gross) gms_gross_target_browsers
        from `etsy-data-warehouse-prod.transaction_mart.transactions_visits` v 
        join exp_browsers b 
            on split(v.visit_id, '.')[offset(0)] = b.browser_id 
            and v.visit_id >= bucket_visit_id
        join `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
            on v.transaction_id = t.transaction_id
        join `etsy-data-warehouse-prod.transaction_mart.all_transactions` t_all
            on v.transaction_id = t_all.transaction_id
            and t_all.creation_tsz >= timestamp_millis(b.bucket_epoch_ms)
            and t_all.date <= end_date
        where
            run_date between unix_seconds(timestamp(start_date)) and unix_seconds(timestamp(end_date))
    ),
    gms_browsers_total as (
        select sum(gms_gross) gms_gross_total
        from `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans`
        where
            date >= start_date
            and date <= end_date
    ),
    browser_traffic_total as (
        select count(distinct browser_id) browsers_total
        from `etsy-data-warehouse-prod.weblog.recent_visits`
        where _date >= start_date
        and _date <= end_date
    ),
    listing_view_traffic_total as (
        select count(distinct browser_id) as count_listing_view_browsers
        from `etsy-data-warehouse-prod.analytics.listing_views` a
        join etsy-data-warehouse-prod.weblog.recent_visits v
            on a.visit_id = v.visit_id
        where a._date between start_date and end_date
        and v._date between start_date and end_date    
    ),
    active_listings_total as (
        select
            count(*) count_active_listings
        from `etsy-data-warehouse-prod.rollups.active_listing_basics` a
    ),
    out1 as (
        select
          array_to_string(bucket_events,',') bucket_events,
          array_to_string(target_events,',') target_events,
          start_date,
          end_date,
          date_diff(end_date, start_date, day) + 1 duration_days,
          count(*) browsers,
          count(*) / (date_diff(end_date, start_date, day) + 1) as browsers_per_day,
          sum(case when c.browser_id is not null then 1 else 0 end) / count(*) as target_event_rate
        from exp_browsers b
        left join target_browsers c
          on b.browser_id = c.browser_id
    )
select a.*,
        gms_gross_target_browsers,
        gms_gross_total,
        gms_gross_target_browsers / gms_gross_total as gms_coverage,
        browsers_total,
        browsers / browsers_total as traffic_coverage,
        browsers / count_listing_view_browsers as listing_view_traffic_coverage,
        count_eligible_listings,
        count_active_listings ,
        count_eligible_listings / count_active_listings  as active_listing_coverage
from out1 a
cross join gms_exp_browsers gtb
cross join gms_browsers_total 
cross join browser_traffic_total
cross join listing_counts
cross join active_listings_total 
cross join listing_view_traffic_total 
;
