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
    listings_with_term_in_desc AS (
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
        coalesce(a.contains_misc_sustainability_term, b.contains_misc_sustainability_term, 0) AS contains_misc_sustainability_term
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
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
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

## CATEGORY COVERAGE##
WITH
    active_listing_counts AS (
        SELECT 
          top_category,
          COUNT(*) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        GROUP BY 1
    ),
    susti_listing_counts AS (
    SELECT
        top_category,
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
    GROUP BY 1
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
JOIN active_listing_counts b USING (top_category)
;


## HORIZONTAL CATEGORY COVERAGE ##
-- is_personalize
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(*) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_personalized = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_personalized = 1
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

--is_vintage
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(*) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_vintage = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_vintage = 1
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

--is_wedding
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(*) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_wedding_title = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_wedding_title = 1
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

-- is_gift
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(*) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_gift_title = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(*) AS count_total_sustainability_listings,
        SUM(term_in_desc) AS count_listings_sustainability_term_in_desc,
        SUM(term_in_title) AS count_listings_sustainability_term_in_title,
        SUM(CASE WHEN term_in_desc + term_in_title = 2 THEN 1 ELSE 0 END) AS count_listings_sustainability_term_in_both_desc_and_title,
        SUM(contains_generic_eco_labels_term) AS count_listings_contains_generic_eco_labels_term,
        SUM(ontains_misc_sustainability_term) AS count_listings_contains_misc_sustainability_term,
        SUM(contains_preferred_manufacturing_term) AS count_listings_contains_preferred_manufacturing_term,
        SUM(contains_sustainable_lifestyle_term) AS count_listings_contains_sustainable_lifestyle_term,
        SUM(contains_sustainable_material_term) AS count_listings_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_gift_title = 1
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

## SHOP COVERAGE ##
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(DISTINCT shop_id) AS count_total_active_shop
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(DISTINCT shop_id) AS count_total_sustainability_shop,
        COUNT(DISTINCT CASE WHEN term_in_desc = 1 THEN shop_id ELSE NULL END) AS count_shop_sustainability_term_in_desc,
        COUNT(DISTINCT CASE WHEN term_in_title = 1 THEN shop_id ELSE NULL END) AS count_shop_sustainability_term_in_title,
        COUNT(DISTINCT CASE WHEN term_in_desc + term_in_title = 2 THEN shop_id ELSE NULL END) AS count_shop_sustainability_term_in_both_desc_and_title,
        COUNT(DISTINCT CASE WHEN contains_generic_eco_labels_term = 1 THEN shop_id ELSE NULL END) AS count_shop_contains_generic_eco_labels_term,
        COUNT(DISTINCT CASE WHEN ontains_misc_sustainability_term = 1 THEN shop_id ELSE NULL END) AS count_shop_contains_misc_sustainability_term,
        COUNT(DISTINCT CASE WHEN contains_preferred_manufacturing_term = 1 THEN shop_id ELSE NULL END) AS count_shop_contains_preferred_manufacturing_term,
        COUNT(DISTINCT CASE WHEN contains_sustainable_lifestyle_term = 1 THEN shop_id ELSE NULL END) AS count_shop_contains_sustainable_lifestyle_term,
        COUNT(DISTINCT CASE WHEN contains_sustainable_material_term = 1 THEN shop_id ELSE NULL END) AS count_shop_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` using (listing_id)
    )
SELECT
    b.count_total_active_shop,
    count_total_sustainability_shop / count_total_active_shop  AS count_total_sustainability_shop_pct_of_active_shop,
    a.*,
    count_shop_sustainability_term_in_desc / count_total_sustainability_shop AS count_shop_sustainability_term_in_desc_pct_of_sustainability_shop,
    count_shop_sustainability_term_in_title / count_total_sustainability_shop AS count_shop_sustainability_term_in_title_pct_of_sustainability_shop,
    count_shop_sustainability_term_in_both_desc_and_title / count_total_sustainability_shop  AS count_shop_sustainability_term_in_both_desc_and_title_pct_of_sustainability_shop,
    count_shop_contains_generic_eco_labels_term / count_total_sustainability_shop AS count_shop_contains_generic_eco_labels_term_pct_of_sustainability_shop,
    count_shop_contains_misc_sustainability_term  / count_total_sustainability_shop AS count_shop_contains_misc_sustainability_term_pct_of_sustainability_shop,
    count_shop_contains_preferred_manufacturing_term / count_total_sustainability_shop AS count_shop_contains_preferred_manufacturing_term_pct_of_sustainability_shop,
    count_shop_contains_sustainable_lifestyle_term / count_total_sustainability_shop AS count_shop_contains_sustainable_lifestyle_term_pct_of_sustainability_shop,
    count_shop_contains_sustainable_material_term / count_total_sustainability_shop AS count_shop_contains_sustainable_material_term_pct_of_sustainability_shop
FROM susti_listing_counts a
CROSS JOIN active_listing_counts b
;




## Sustainability Term Listing Counts by Term ##
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
        FROM listings_with_term_in_desc b
    )
SELECT 
    term, 
    COUNT(DISTINCT listing_id) AS count_listings
FROM combined 
GROUP BY 1
ORDER BY 2 desc
;




