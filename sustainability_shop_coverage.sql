## ACTIVE SHOPS ##
-- OVERALL, LISTING FIELDS, TERM CATEGORIES
WITH
    active_shop_counts AS (
        SELECT 
          COUNT(DISTINCT shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
    ),
    susti_shop_counts AS (
    SELECT
        COUNT(distinct shop_id) AS count_total_sustainability_shops,
        COUNT(distinct CASE WHEN term_in_desc = 1 THEN shop_id ELSE NULL END) AS count_shops_sustainability_term_in_desc,
        COUNT(distinct CASE WHEN term_in_title = 1 THEN shop_id ELSE NULL END) AS count_shops_sustainability_term_in_title,
        COUNT(distinct CASE WHEN term_in_desc + term_in_title = 2 THEN shop_id ELSE NULL END) AS count_shops_sustainability_term_in_both_desc_and_title,
        COUNT(distinct CASE WHEN contains_generic_eco_labels_term = 1 THEN shop_id ELSE NULL END) AS count_shops_contains_generic_eco_labels_term,
        COUNT(distinct CASE WHEN contains_misc_sustainability_term = 1 THEN shop_id ELSE NULL END) AS count_shops_contains_misc_sustainability_term,
        COUNT(distinct CASE WHEN contains_preferred_manufacturing_term = 1 THEN shop_id ELSE NULL END) AS count_shops_contains_preferred_manufacturing_term,
        COUNT(distinct CASE WHEN contains_sustainable_lifestyle_term = 1 THEN shop_id ELSE NULL END) AS count_shops_contains_sustainable_lifestyle_term,
        COUNT(distinct CASE WHEN contains_sustainable_material_term = 1 THEN shop_id ELSE NULL END) AS count_shops_contains_sustainable_material_term
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
    )
SELECT
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM susti_shop_counts a
CROSS JOIN active_shop_counts b
;

-- TOP CATEGORY
WITH
    cat_active_shop_counts AS (
        SELECT 
          top_category,
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        GROUP BY 1
    ),
    cat_susti_shop_counts AS (
    SELECT
        top_category,
        COUNT(DISTINCT shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` USING (listing_id)
    GROUP BY 1
    )
SELECT
    a.top_category,
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM cat_susti_shop_counts a
JOIN cat_active_shop_counts b USING (top_category)
;


-- HORIZONTAL CATEGORY
    -- is_personalize
WITH
    hcat_personalize_active_shop_count AS (
        SELECT 
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_personalized = 1
    ),
    hcat_personalize_susti_shop_counts AS (
    SELECT
        COUNT(distinct shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN  `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id) 
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_personalized = 1
    )
SELECT
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM hcat_personalize_susti_shop_counts a
CROSS JOIN hcat_personalize_active_shop_count b
;

    -- is_vintage
WITH
    hcat_vintage_active_shop_counts AS (
        SELECT 
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_vintage = 1
    ),
    hcat_vintage_susti_shop_counts AS (
    SELECT
        COUNT(distinct shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN  `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id) 
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_vintage = 1
    )
SELECT
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM hcat_vintage_susti_shop_counts a
CROSS JOIN hcat_vintage_active_shop_counts b
;

    -- is_wedding
WITH
    hcat_wedding_active_shop_counts AS (
        SELECT 
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_wedding_title = 1
    ),
    hcat_wedding_susti_shop_counts AS (
    SELECT
        COUNT(distinct shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN  `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id) 
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_wedding_title = 1
    )
SELECT
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM hcat_wedding_susti_shop_counts a
CROSS JOIN  hcat_wedding_active_shop_counts b
;

-- is_gift
WITH
    hcat_gift_active_shop_counts AS (
        SELECT 
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
        JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
        WHERE h.is_gift_title = 1
    ),
    hcat_gift_susti_shop_counts AS (
    SELECT
        COUNT(distinct shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN  `etsy-data-warehouse-prod.rollups.active_listing_basics` a USING (listing_id) 
    JOIN  `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy` h USING (listing_id)
    WHERE h.is_gift_title = 1
    )
SELECT
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM hcat_gift_susti_shop_counts a
CROSS JOIN hcat_gift_active_shop_counts b
;

-- VERTICAL CATEGORY
with v_cat AS (select
	listing_id,
    shop_id,
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
    active_shop_counts AS (
        SELECT 
          vertical_category,
          COUNT(distinct shop_id) AS count_total_active_shops
        FROM v_cat
        GROUP BY 1
    ),
    susti_shop_counts AS (
    SELECT
        vertical_category,
        COUNT(distinct shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN v_cat USING (listing_id)
    GROUP BY 1
    )
SELECT
    a.vertical_category,
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM susti_shop_counts a
JOIN active_shop_counts b USING (vertical_category)
;

-- TERMS
WITH
    terms AS(
        SELECT *
        FROM `etsy-data-warehouse-dev.nlao.sustainability_term`
        WHERE term NOT IN ('green','vintage')
    ),
    active_listing_desc AS(
        SELECT 
            a.listing_id,
            a.description,
            a.title,
            b.shop_id
        FROM `etsy-data-warehouse-prod.etsy_shard.listings` a
        JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` b
            ON a.listing_id = b.listing_id
        WHERE a.description IS NOT NULL
    ),
    searchable_desc_and_titles AS (
        SELECT
            listing_id,
            shop_id,
            ' '|| LOWER(REGEXP_REPLACE(description, '[^A-Za-z0-9-]', ' '))|| ' ' AS description_searchable,
            ' '|| LOWER(REGEXP_REPLACE(title, '[^A-Za-z0-9-]', ' '))|| ' ' AS title_searchable
        FROM active_listing_desc
    ),
    listings_with_term_in_desc AS(
        SELECT DISTINCT 
            d.listing_id,
            d.shop_id,
            t.term,
            'description' AS location
        FROM searchable_desc_and_titles d
        JOIN terms t
            ON REGEXP_CONTAINS(d.description_searchable, ' '|| t.term || ' ')
    ),
    listings_with_term_in_title AS(
        SELECT DISTINCT
            d.listing_id,
            d.shop_id,
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
    COUNT(DISTINCT listing_id) AS count_listings,
    COUNT(DISTINCT shop_id) AS count_shops
FROM combined 
GROUP BY 1
ORDER BY 2 desc
;

-- SELLER TIER
WITH
    seller_tier_active_shop_counts AS (
        SELECT 
          seller_tier,
          COUNT(DISTINCT shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
        GROUP BY 1
    ),
    seller_tier_susti_shop_counts AS (
    SELECT
        seller_tier,
        COUNT(DISTINCT shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` using (listing_id)
    JOIN `etsy-data-warehouse-prod.rollups.seller_basics` USING (shop_id)
    GROUP BY 1
    )
SELECT
    a.seller_tier,
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM seller_tier_susti_shop_counts a
JOIN seller_tier_active_shop_counts b USING (seller_tier)
ORDER BY 1 ASC
;

-- SELLER COUNTRY
WITH
    seller_country_active_shop_counts AS (
        SELECT 
          country_name,
          COUNT(DISTINCT shop_id) AS count_total_active_shops
        FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
        GROUP BY 1
    ),
    seller_country_susti_shop_counts AS (
    SELECT
        country_name,
        COUNT(DISTINCT shop_id) AS count_total_sustainability_shops
    FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`
    JOIN `etsy-data-warehouse-prod.rollups.active_listing_basics` using (listing_id)
    GROUP BY 1
    )
SELECT
    a.country_name,
    a.count_total_sustainability_shops,
    b.count_total_active_shops
FROM seller_country_susti_shop_counts a
JOIN seller_country_active_shop_counts b USING (country_name)
;
