CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_sampling_audience` AS (
WITH can_be_contacted_feedback as (
  select 
    ui.user_id 
  from 
    etsy-data-warehouse-prod.etsy_index.users_index ui
  where
    ui.is_seller = 1 -- they are a seller
    and ui.is_admin = 0 -- who is not an etsy admin
    and ui.is_nipsa = 0 -- and aren't in trouble with etsy
    and ui.is_frozen = 0 -- or frozen
    and ui.user_state = 'active' -- and they're active
    -- and they haven't been contacted in 3 months for research:
    and ui.user_id not in (
      select distinct 
        user_id
      from 
        etsy-data-warehouse-prod.research.ux_participants
      where 
        -- can change this to 2 month if need more sample, 
        -- but check the dashboard first
        date(date_contacted) >= date_sub(current_date, interval 3 month)
    )
    -- who have agreed to give us feedback:
    and ui.user_id in (
      select distinct 
        user_id
      from 
        etsy-data-warehouse-prod.rollups.email_subscribers
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
        etsy-data-warehouse-prod.etsy_shard.user_preferences
      where 
        preference_id = 704 
        and preference_setting = 'false'
    )
    -- and who isn't on vacation:
    and ui.user_id in (
      select distinct 
        user_id
      from 
        etsy-data-warehouse-prod.etsy_shard.shop_data
      where
        is_vacation = 0
    )
)
,top_5_gms_listing_w_label_change AS (
SELECT
a.user_id,
a.listing_id,
a.past_year_gms,
DENSE_RANK() OVER (PARTITION BY a.shop_id ORDER BY a.past_year_gms DESC) AS in_shop_gms_rank,
b.previous_backend_label,
b.previous_frontend_handmade,
b.how_its_made_label,
a.title,
a.top_category,
t.full_path,
i.image_url1
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
JOIN `etsy-data-warehouse-prod.rollups.how_its_made_active_listing_label` b
  ON a.listing_id = b.listing_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON a.taxonomy_id = t.taxonomy_id
JOIN `etsy-data-warehouse-prod.listing_mart.listing_images_active` i
  ON i.listing_id = a.listing_id
WHERE past_year_gms > 0
AND how_its_made_label_reason !="No label" -- filter out no label listings
AND (
  (is_vintage = 1 AND how_its_made_label_reason != "Vintage") -- filter out listing that are previously has vintage table and now it has "handpicked by" label bc it's vintage
  OR (b.previous_frontend_handmade = 1 AND b.how_its_made_label != "Made by") -- filter out listing that are previously handmade and now it has "made by" label bc it's handmade
  OR (b.previous_frontend_handmade = 0 AND how_its_made_label_reason !="No label") -- filter in listings that has no handmade label and now it has something
)
)
SELECT
  a.user_id,
  b.primary_email as Email, -- qualtrics requires the email column to be Email
  b.shop_id,
  b.shop_name, -- always grab this because it's in the email invite template
  b.top_category_new,
  b.seller_tier_new,
  -- add other variables from seller basics with b.variable_name, as necessary
  c.listing_id,
  c.past_year_gms,
  c.in_shop_gms_rank,
  c.previous_backend_label,
  c.previous_frontend_handmade,
  c.how_its_made_label,
  c.title,
  c.top_category,
  c.full_path,
  c.image_url1
FROM can_be_contacted_feedback a
JOIN `etsy-data-warehouse-prod.rollups.seller_basics` b 
  ON a.user_id = b.user_id
JOIN top_5_gms_listing_w_label_change c
  ON c.user_id = b.user_id
WHERE
  -- limit to those with active listings and a sale in the past 12mo
  b.seller_tier_new in ('Small Shop', 'Medium Shop', 'Top Shop', 'Power Shop')
  and b.active_listings > 4
  -- and united states, since that's what we usually sample
  -- you can remove this or replace countries for int'l sample
  and b.country_name = 'United States'
  -- get listings with a sale
  AND b.past_year_gms > 0
  -- get the top 5 selling listings if there's more than 5
  AND c.in_shop_gms_rank<=5
ORDER BY 
  user_id, in_shop_gms_rank
)
;

-- TAKE SAMPLE
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.him_sampling_audience_40000` AS (
WITH design_sample_20000 AS (
SELECT
DISTINCT user_id
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience`
WHERE how_its_made_label = "Designed by"
ORDER BY RAND()
LIMIT 30000
)
,source_sample_5000 AS (
SELECT
DISTINCT user_id
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience`
WHERE how_its_made_label = "Sourced by"
AND user_id NOT IN (SELECT user_id FROM design_sample_20000)
ORDER BY RAND()
LIMIT 6000
)
, handpicked_sample_5000 AS (
SELECT
DISTINCT user_id
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience`
WHERE how_its_made_label = "Handpicked by"
AND user_id NOT IN (SELECT user_id FROM design_sample_20000)
AND user_id NOT IN (SELECT user_id FROM source_sample_5000)
ORDER BY RAND()
LIMIT 6000
)
, agg AS (
SELECT user_id FROM design_sample_20000
UNION ALL
SELECT user_id FROM source_sample_5000
UNION ALL
SELECT user_id FROM handpicked_sample_5000
)
SELECT
*
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience` a
JOIN agg b USING(user_id)
ORDER BY a.user_id, a.in_shop_gms_rank
)
;


SELECT
previous_frontend_handmade,
how_its_made_label,
count(distinct user_id)
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience_40000`
GROUP BY 1,2
-- ORDER BY a.user_id, a.in_shop_gms_rank
;

SELECT
COUNT(DISTINCT USER_ID)
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience_40000`
;


SELECT
*
FROM `etsy-data-warehouse-dev.nlao.him_sampling_audience_40000`
;


