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
          COUNT(distinct listing_id) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_personalized = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(distinct listing_id) AS count_total_sustainability_listings
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_personalized = 1
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*
FROM susti_listing_counts a
CROSS JOIN active_listing_counts b
;

--is_vintage
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(distinct listing_id) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_vintage = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(distinct listing_id) AS count_total_sustainability_listings
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_vintage = 1
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*
FROM susti_listing_counts a
CROSS JOIN active_listing_counts b
;

--is_wedding
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(distinct listing_id) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_wedding_title = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(distinct listing_id) AS count_total_sustainability_listings
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_wedding_title = 1
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*
FROM susti_listing_counts a
CROSS JOIN active_listing_counts b
;

-- is_gift
WITH
    active_listing_counts AS (
        SELECT 
          COUNT(distinct listing_id) AS count_total_active_listings
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_gift_title = 1
    ),
    susti_listing_counts AS (
    SELECT
        COUNT(distinct listing_id) AS count_total_sustainability_listings
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_gift_title = 1
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*
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

## SHOP COVERAGE - SELLER TIER ##
WITH
    active_listing_counts AS (
        SELECT 
          seller_tier,
          COUNT(DISTINCT shop_id) AS count_total_active_shop
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
        GROUP BY 1
    ),
    susti_listing_counts AS (
    SELECT
        seller_tier,
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
    JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
    GROUP BY 1
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
JOIN active_listing_counts b USING (seller_tier)
;

## SHOP COVERAGE - SELLER COUNTRY ##
WITH
    active_listing_counts AS (
        SELECT 
          country_name,
          COUNT(DISTINCT shop_id) AS count_total_active_shop
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        GROUP BY 1
    ),
    susti_listing_counts AS (
    SELECT
        country_name,
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
    GROUP BY 1
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
JOIN active_listing_counts b USING (country_name)
;


## VERTICAL CATEGORY ##
with v_cat AS (select
	listing_id,
    case when t.path in ("artist_trading_cards", "clip_art", "collectibles", "dolls_and_miniatures", "drawing_and_illustration", "fiber_arts", 
		"glass_art", "mixed_media_and_collage", "painting", "photography", "prints", "sculpture") 
		OR (a.top_category="art_and_collectibles" and t.path="") then "art"
	when t.path in ("patches_and_pins", "body_jewelry", "bracelets", "brooches", "dress_and_shoe_clips", "earrings", "jewelry_sets", 
		"necklaces",  "rings", "sweater_clips", "watches", "wearable_tech_jewelry") 
		or (a.top_category="jewelry" and t.path="") then "jewelry"
	when t.path in ("baby_accessories", "baby_and_child_care", "boys_clothing", "girls_clothing", "unisex_kids_clothing", "boys_shoes", "girls_shoes", 
		"unisex_kids_shoes", "games_and_puzzles", "sports_and_outdoor_games", "toys", "diaper_bags") 
		or (a.top_category="toys_and_games" and t.path="") then "kids_and_baby"
	when t.path in ("belts_and_suspenders", "costume_accessories", "gloves_and_mittens", "hats_and_caps", "keychains_and_lanyards", 
		"scarves_and_wraps", "sunglasses_and_eyewear", "umbrellas_and_rain_accessories", "accessory_cases", "backpacks", "fanny_packs", 
		"hair_accessories", "handbags", "luggage_and_travel", "market_bags", "mens_clothing", "mens_shoes", "messenger_bags", "pouches_and_coin_purses", 
		"sports_bags", "suit_and_tie_accessories","totes", "wallets_and_money_clips", "unisex_adult_clothing", "insoles_and_accessories", 
		"unisex_adult_shoes", "womens_clothing", "womens_shoes")
        or (a.top_category in ("accessories", "bags_and_purses", "clothing", "shoes") and t.path="") then "fashion"
    when a.top_category = "weddings" then "weddings"
    when a.top_category in ("paper_and_party_supplies", "craft_supplies_and_tools") then "supplies"
    when a.top_category = "other" then "other"
    else "home_and_living" 
    end as vertical_category
from 
	`etsy-data-warehouse-prod.structured_data.taxonomy` t
join 
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (taxonomy_id)
),
    active_listing_counts AS (
        SELECT 
          vertical_category,
          COUNT(distinct listing_id) AS count_total_active_listings
        FROM v_cat
        GROUP BY 1
    ),
    susti_listing_counts AS (
    SELECT
        vertical_category,
        COUNT(distinct listing_id) AS count_total_sustainability_listings
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`
    JOIN v_cat USING (listing_id)
    GROUP BY 1
    )
SELECT
    b.count_total_active_listings,
    count_total_sustainability_listings / count_total_active_listings  AS count_total_sustainability_listings_pct_of_active_listings,
    a.*
FROM susti_listing_counts a
JOIN active_listing_counts b USING (vertical_category)
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


##### LISTING VIEW & VISIT #########
-- SUB CATEGORY
SELECT
COUNT(visit_id) AS view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN visit_id ELSE NULL END) AS sub_listing_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc = 1) THEN visit_id ELSE NULL END) AS term_in_desc_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title = 1) THEN visit_id ELSE NULL END) AS term_in_title_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title + term_in_desc = 2) THEN visit_id ELSE NULL END) AS term_in_both_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_material_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_material_term_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_generic_eco_labels_term = 1) THEN visit_id ELSE NULL END) AS contains_generic_eco_labels_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_lifestyle_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_lifestyle_terms_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_preferred_manufacturing_term = 1) THEN visit_id ELSE NULL END) AS contains_preferred_manufacturing_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE ontains_misc_sustainability_term = 1) THEN visit_id ELSE NULL END) AS contains_misc_sustainability_term_view_count,
COUNT(DISTINCT visit_id) AS visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN visit_id ELSE NULL END) AS sub_listing_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc = 1) THEN visit_id ELSE NULL END) AS term_in_desc_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title = 1) THEN visit_id ELSE NULL END) AS term_in_title_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title + term_in_desc = 2) THEN visit_id ELSE NULL END) AS term_in_both_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_material_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_material_term_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_generic_eco_labels_term = 1) THEN visit_id ELSE NULL END) AS contains_generic_eco_labels_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_lifestyle_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_lifestyle_terms_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_preferred_manufacturing_term = 1) THEN visit_id ELSE NULL END) AS contains_preferred_manufacturing_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE ontains_misc_sustainability_term = 1) THEN visit_id ELSE NULL END) AS contains_misc_sustainability_term_visit_count
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
WHERE a._date between '2022-04-23' and '2022-05-23'
;

-- TOP_CATEGORY 
SELECT
top_category,
COUNT(visit_id) AS view_count,
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN visit_id ELSE NULL END) AS sub_listing_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN visit_id ELSE NULL END) AS sub_listing_visit_count
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
GROUP BY 1
;

-- HORIZONTAL CATEGORY
WITH horizontal_all_view_count AS (
SELECT
COUNT(CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS perso_view_count,
COUNT(CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS wedding_view_count,
COUNT(CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS gift_view_count,
COUNT(CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS vintage_view_count,
COUNT(DISTINCT CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS perso_visit_count,
COUNT(DISTINCT CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS wedding_visit_count,
COUNT(DISTINCT CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS gift_visit_count,
COUNT(DISTINCT CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS vintage_visit_count
FROM `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy`
JOIN `etsy-data-warehouse-prod.analytics.listing_views` a USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
),
horitonzal_sub_view_count AS (
SELECT
COUNT(CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS perso_view_count,
COUNT(CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS wedding_view_count,
COUNT(CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS gift_view_count,
COUNT(CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS vintage_view_count,
COUNT(DISTINCT CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS perso_visit_count,
COUNT(DISTINCT CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS wedding_visit_count,
COUNT(DISTINCT CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS gift_visit_count,
COUNT(DISTINCT CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS vintage_visit_count
FROM `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy`
JOIN `etsy-data-warehouse-prod.analytics.listing_views` a USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
  AND listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) 
)
SELECT
a.*,
b.*
FROM horizontal_all_view_count a
CROSS JOIN horitonzal_sub_view_count b
;

-- VERTICAL CATEGORY
with v_cat AS (
SELECT
	listing_id,
    case when t.path in ("artist_trading_cards", "clip_art", "collectibles", "dolls_and_miniatures", "drawing_and_illustration", "fiber_arts", 
		"glass_art", "mixed_media_and_collage", "painting", "photography", "prints", "sculpture") 
		OR (a.top_category="art_and_collectibles" and t.path="") then "art"
	when t.path in ("patches_and_pins", "body_jewelry", "bracelets", "brooches", "dress_and_shoe_clips", "earrings", "jewelry_sets", 
		"necklaces",  "rings", "sweater_clips", "watches", "wearable_tech_jewelry") 
		or (a.top_category="jewelry" and t.path="") then "jewelry"
	when t.path in ("baby_accessories", "baby_and_child_care", "boys_clothing", "girls_clothing", "unisex_kids_clothing", "boys_shoes", "girls_shoes", 
		"unisex_kids_shoes", "games_and_puzzles", "sports_and_outdoor_games", "toys", "diaper_bags") 
		or (a.top_category="toys_and_games" and t.path="") then "kids_and_baby"
	when t.path in ("belts_and_suspenders", "costume_accessories", "gloves_and_mittens", "hats_and_caps", "keychains_and_lanyards", 
		"scarves_and_wraps", "sunglasses_and_eyewear", "umbrellas_and_rain_accessories", "accessory_cases", "backpacks", "fanny_packs", 
		"hair_accessories", "handbags", "luggage_and_travel", "market_bags", "mens_clothing", "mens_shoes", "messenger_bags", "pouches_and_coin_purses", 
		"sports_bags", "suit_and_tie_accessories","totes", "wallets_and_money_clips", "unisex_adult_clothing", "insoles_and_accessories", 
		"unisex_adult_shoes", "womens_clothing", "womens_shoes")
        or (a.top_category in ("accessories", "bags_and_purses", "clothing", "shoes") and t.path="") then "fashion"
    when a.top_category = "weddings" then "weddings"
    when a.top_category in ("paper_and_party_supplies", "craft_supplies_and_tools") then "supplies"
    when a.top_category = "other" then "other"
    else "home_and_living" 
    end as vertical_category
from 
	`etsy-data-warehouse-prod.structured_data.taxonomy` t
join 
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (taxonomy_id)
),
active_view_counts AS (
      SELECT 
      vertical_category,
      COUNT(visit_id) as all_view_count,
      COUNT(DISTINCT visit_id) as all_visit_count
    FROM `etsy-data-warehouse-prod.analytics.listing_views` a
    JOIN v_cat USING (listing_id)
    WHERE a._date between '2022-04-23' and '2022-05-23'
    GROUP BY 1
    ),
susti_view_counts AS (
    SELECT 
      vertical_category,
      COUNT(visit_id) as sub_view_count,
      COUNT(DISTINCT visit_id) as sub_visit_count
    FROM `etsy-data-warehouse-prod.analytics.listing_views` a
    JOIN v_cat USING (listing_id)
    WHERE a._date between '2022-04-23' and '2022-05-23'
    AND listing_id in (select listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)
    GROUP BY 1
)
SELECT
    a.*,
    b.*
FROM active_view_counts a
JOIN susti_view_counts b USING (vertical_category)
;

-- SELLER COUNTRY
SELECT
country_name,
COUNT(visit_id) AS view_count,
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN visit_id ELSE NULL END) AS sub_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN visit_id ELSE NULL END) AS sub_visit_count,
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
JOIN `etsy-data-warehouse-prod.listing_mart.listings` USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
GROUP BY 1
;

-- SELLER TIER
SELECT
seller_tier,
COUNT(visit_id) AS view_count,
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN visit_id ELSE NULL END) AS sub_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN visit_id ELSE NULL END) AS sub_visit_count,
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
JOIN `etsy-data-warehouse-prod.listing_mart.listings` USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
GROUP BY 1
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
    COUNT(visit_id) AS count_listings,
    COUNT(DISTINCT visit_id) AS count_listings
FROM combined
JOIN `etsy-data-warehouse-prod.analytics.listing_views` a USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23' 
GROUP BY 1
ORDER BY 2 desc
;

######## %transaction, % gross gms ########
declare start_date date default '2022-04-23';
declare end_date date default '2022-05-23';

-- LISTING FIELDS, TERM CATEGORY
SELECT
COUNT(DISTINCT transaction_id) AS transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN transaction_id ELSE NULL END) AS sub_listing_transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc = 1) THEN transaction_id ELSE NULL END) AS term_in_desc_transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title = 1) THEN transaction_id ELSE NULL END) AS term_in_title_transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title + term_in_desc = 2) THEN transaction_id ELSE NULL END) AS term_in_both_transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_material_term = 1) THEN transaction_id ELSE NULL END) AS contains_sustainable_material_term_transaction_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_generic_eco_labels_term = 1) THEN transaction_id ELSE NULL END) AS contains_generic_eco_labels_term_transaction_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_lifestyle_term = 1) THEN transaction_id ELSE NULL END) AS contains_sustainable_lifestyle_terms_term_transaction_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_preferred_manufacturing_term = 1) THEN transaction_id ELSE NULL END) AS contains_preferred_manufacturing_term_transaction_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE ontains_misc_sustainability_term = 1) THEN transaction_id ELSE NULL END) AS contains_misc_sustainability_term_transaction_count, 
SUM(t.gms_gross) AS gms_gross_total,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN t.gms_gross ELSE 0 END) AS sub_listing_gms_gross_count,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc = 1) THEN t.gms_gross ELSE 0 END) AS term_in_desc_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title = 1) THEN t.gms_gross ELSE 0 END) AS term_in_title_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title + term_in_desc = 2) THEN t.gms_gross ELSE 0 END) AS term_in_both_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_material_term = 1) THEN t.gms_gross ELSE 0 END) AS contains_sustainable_material_term_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_generic_eco_labels_term = 1) THEN t.gms_gross ELSE 0 END) AS contains_generic_eco_labels_term_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_lifestyle_term = 1) THEN t.gms_gross ELSE 0 END) AS contains_sustainable_lifestyle_term_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_preferred_manufacturing_term = 1) THEN t.gms_gross ELSE 0 END) AS contains_preferred_manufacturing_term_gms_gross_target,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE ontains_misc_sustainability_term = 1) THEN t.gms_gross ELSE 0 END) AS contains_misc_sustainability_term_gms_gross_target,
FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING (transaction_id)
WHERE a.date between start_date and end_date
;

-- TOP_CATEGORY
SELECT
new_category,
COUNT(DISTINCT transaction_id) AS transaction_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN transaction_id ELSE NULL END) AS sub_transaction_count,
SUM(t.gms_gross) AS gms_gross_total,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN t.gms_gross ELSE 0 END) AS sub_listing_gms_gross_count
FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING (transaction_id)
WHERE a.date between start_date and end_date
GROUP BY 1
;

-- HORIZONTAL CATEGORY
WITH horizontal_all_trans_count AS (
SELECT
COUNT(DISTINCT CASE WHEN is_personalized = 1 THEN transaction_id ELSE NULL END) AS perso_trans_count,
COUNT(DISTINCT CASE WHEN is_wedding_title = 1 THEN transaction_id ELSE NULL END) AS wedding_trans_count,
COUNT(DISTINCT CASE WHEN is_gift_title = 1 THEN transaction_id ELSE NULL END) AS gift_trans_count,
COUNT(DISTINCT CASE WHEN is_vintage = 1 THEN transaction_id ELSE NULL END) AS vintage_trans_count,
SUM( CASE WHEN is_personalized = 1 THEN gms_gross ELSE NULL END) AS perso_gms_gross,
SUM( CASE WHEN is_wedding_title = 1 THEN gms_gross ELSE NULL END) AS wedding_gms_gross,
SUM( CASE WHEN is_gift_title = 1 THEN gms_gross ELSE NULL END) AS gift_gms_gross,
SUM( CASE WHEN is_vintage = 1 THEN gms_gross ELSE NULL END) AS vintage_gms_gross
FROM `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy`
JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` a USING (transaction_id)
WHERE a.date between start_date and end_date
),
horitonzal_sub_trans_count AS (
SELECT
COUNT(DISTINCT CASE WHEN is_personalized = 1 THEN transaction_id ELSE NULL END) AS sub_perso_trans_count,
COUNT(DISTINCT CASE WHEN is_wedding_title = 1 THEN transaction_id ELSE NULL END) AS sub_wedding_trans_count,
COUNT(DISTINCT CASE WHEN is_gift_title = 1 THEN transaction_id ELSE NULL END) AS sub_gift_trans_count,
COUNT(DISTINCT CASE WHEN is_vintage = 1 THEN transaction_id ELSE NULL END) AS sub_vintage_trans_count,
SUM( CASE WHEN is_personalized = 1 THEN gms_gross ELSE NULL END) AS perso_gms_gross,
SUM( CASE WHEN is_wedding_title = 1 THEN gms_gross ELSE NULL END) AS wedding_gms_gross,
SUM( CASE WHEN is_gift_title = 1 THEN gms_gross ELSE NULL END) AS gift_gms_gross,
SUM( CASE WHEN is_vintage = 1 THEN gms_gross ELSE NULL END) AS vintage_gms_gross,
FROM `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy`
JOIN `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` a USING (transaction_id)
WHERE listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) 
  AND a.date between start_date and end_date
)
SELECT
a.*,
b.*
FROM horizontal_all_trans_count a
CROSS JOIN horitonzal_sub_trans_count b
;

-- VERTICAL CATEGORY 
with v_cat AS (
SELECT
	listing_id,
    case when t.path in ("artist_trading_cards", "clip_art", "collectibles", "dolls_and_miniatures", "drawing_and_illustration", "fiber_arts", 
		"glass_art", "mixed_media_and_collage", "painting", "photography", "prints", "sculpture") 
		OR (a.top_category="art_and_collectibles" and t.path="") then "art"
	when t.path in ("patches_and_pins", "body_jewelry", "bracelets", "brooches", "dress_and_shoe_clips", "earrings", "jewelry_sets", 
		"necklaces",  "rings", "sweater_clips", "watches", "wearable_tech_jewelry") 
		or (a.top_category="jewelry" and t.path="") then "jewelry"
	when t.path in ("baby_accessories", "baby_and_child_care", "boys_clothing", "girls_clothing", "unisex_kids_clothing", "boys_shoes", "girls_shoes", 
		"unisex_kids_shoes", "games_and_puzzles", "sports_and_outdoor_games", "toys", "diaper_bags") 
		or (a.top_category="toys_and_games" and t.path="") then "kids_and_baby"
	when t.path in ("belts_and_suspenders", "costume_accessories", "gloves_and_mittens", "hats_and_caps", "keychains_and_lanyards", 
		"scarves_and_wraps", "sunglasses_and_eyewear", "umbrellas_and_rain_accessories", "accessory_cases", "backpacks", "fanny_packs", 
		"hair_accessories", "handbags", "luggage_and_travel", "market_bags", "mens_clothing", "mens_shoes", "messenger_bags", "pouches_and_coin_purses", 
		"sports_bags", "suit_and_tie_accessories","totes", "wallets_and_money_clips", "unisex_adult_clothing", "insoles_and_accessories", 
		"unisex_adult_shoes", "womens_clothing", "womens_shoes")
        or (a.top_category in ("accessories", "bags_and_purses", "clothing", "shoes") and t.path="") then "fashion"
    when a.top_category = "weddings" then "weddings"
    when a.top_category in ("paper_and_party_supplies", "craft_supplies_and_tools") then "supplies"
    when a.top_category = "other" then "other"
    else "home_and_living" 
    end as vertical_category
from 
	`etsy-data-warehouse-prod.structured_data.taxonomy` t
join 
	`etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (taxonomy_id)
),
active_trans_gms_counts AS (
      SELECT 
      vertical_category,
      COUNT(transaction_id) as transaction_count,
      SUM(gms_gross) as gms_gross
    FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans`
    JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING (transaction_id)
    JOIN v_cat USING (listing_id)
    WHERE a.date between start_date and end_date
    GROUP BY 1
    ),
susti_trans_gms_counts AS (
    SELECT 
      vertical_category,
      COUNT(transaction_id) as transaction_count,
      SUM(gms_gross) as gms_gross
    FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans`
    JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING (transaction_id)
    JOIN v_cat USING (listing_id) 
    WHERE listing_id in (select listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)
          AND a.date between start_date and end_date
    GROUP BY 1
)
SELECT
    a.*,
    b.*
FROM active_trans_gms_counts a
JOIN susti_trans_gms_counts b USING (vertical_category)
;

-- SELLER COUNTRY
SELECT
seller_country,
COUNT(DISTINCT transaction_id) AS transaction_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`)THEN transaction_id ELSE NULL END) AS sub_transaction_count,
SUM(t.gms_gross) AS gms_gross_total,
SUM(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN t.gms_gross ELSE 0 END) AS sub_listing_gms_gross_count
FROM `etsy-data-warehouse-prod.transaction_mart.transactions_gms_by_trans` t
JOIN `etsy-data-warehouse-prod.transaction_mart.all_transactions` a USING (transaction_id)
WHERE a.date between start_date and end_date
GROUP BY 1
;




