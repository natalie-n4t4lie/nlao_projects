
-- CREATE CONVERISON RATE TABLE
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_cr` AS (
with visit_level AS (  
SELECT
visit_id,
listing_id,
max(purchased_in_visit) as purchased,
FROM `etsy-data-warehouse-prod.analytics.listing_views`
WHERE _date BETWEEN '2022-05-15' AND '2022-06-15'
GROUP BY 1,2
)
SELECT
listing_id,
COUNT(distinct visit_id) AS visit_count,
SUM(purchased) AS purchase_count,
SUM(purchased) / COUNT(distinct visit_id) AS conversion_rate
FROM visit_level
GROUP BY 1
)
;

====================================
---- WITHOUT VINTAGE KEYWORDS ------
====================================
-- OVERALL
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

-- listing field
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_desc = 1) THEN 1 ELSE 0 END AS sus_desc_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_title = 1) THEN 1 ELSE 0 END AS sus_title_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_desc + term_in_title = 2) THEN 1 ELSE 0 END AS sus_both_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

-- sustainable category
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_generic_eco_labels_term = 1) THEN 1 ELSE 0 END AS generic_eco_labels_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_misc_sustainability_term = 1) THEN 1 ELSE 0 END AS contains_misc_sustainability_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_preferred_manufacturing_term = 1) THEN 1 ELSE 0 END AS preferred_manufacturing_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_lifestyle_term = 1) THEN 1 ELSE 0 END AS sustainable_lifestyle_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_material_term = 1) THEN 1 ELSE 0 END AS sustainable_material_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

--top category
SELECT
l.top_category,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sustainable_material_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` l USING (listing_id)
GROUP BY 1,2
;

-- vertical category
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
)

SELECT
vertical_category,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM v_cat
JOIN `etsy-data-warehouse-dev.nlao.listing_cr` USING (listing_id)
GROUP BY 1,2
ORDER BY 1 ASC, 2 ASC
;

-- term
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
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM combined
JOIN `etsy-data-warehouse-dev.nlao.listing_cr` USING (listing_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;

-- SELLER TIER
SELECT
seller_tier,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr` 
JOIN `etsy-data-warehouse-prod.listing_mart.listings` l USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics_all` s USING (user_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;


-- SELLER COUNTRY
SELECT
CASE WHEN REGEXP_CONTAINS(country_name, r'Australia|Canada|France|Germany|India|United Kingdom|United States') THEN country_name
ELSE 'ROW' END AS country_name,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr` 
JOIN `etsy-data-warehouse-prod.listing_mart.listings` l USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics_all` s USING (user_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;

====================================
----- WITH VINTAGE KEYWORDS --------
====================================
-- OVERALL
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

-- listing field
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc = 1) THEN 1 ELSE 0 END AS sus_desc_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_title = 1) THEN 1 ELSE 0 END AS sus_title_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE term_in_desc + term_in_title = 2) THEN 1 ELSE 0 END AS sus_both_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

-- sustainable category
SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_generic_eco_labels_term = 1) THEN 1 ELSE 0 END AS generic_eco_labels_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE ontains_misc_sustainability_term = 1) THEN 1 ELSE 0 END AS contains_misc_sustainability_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_preferred_manufacturing_term = 1) THEN 1 ELSE 0 END AS preferred_manufacturing_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_lifestyle_term = 1) THEN 1 ELSE 0 END AS sustainable_lifestyle_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

SELECT
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings` WHERE contains_sustainable_material_term = 1) THEN 1 ELSE 0 END AS sustainable_material_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
GROUP BY 1
;

--top category
SELECT
l.top_category,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sustainable_material_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr`
JOIN `etsy-data-warehouse-prod.listing_mart.listing_vw` l USING (listing_id)
GROUP BY 1,2
;

-- vertical category
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
)

SELECT
vertical_category,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM v_cat
JOIN `etsy-data-warehouse-dev.nlao.listing_cr` USING (listing_id)
GROUP BY 1,2
ORDER BY 1 ASC, 2 ASC
;

-- term
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
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM combined
JOIN `etsy-data-warehouse-dev.nlao.listing_cr` USING (listing_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;

--SELLER COUNTRY

SELECT
CASE WHEN REGEXP_CONTAINS(country_name, r'Australia|Canada|France|Germany|India|United Kingdom|United States') THEN country_name
ELSE 'ROW' END AS country_name,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr` 
JOIN `etsy-data-warehouse-prod.listing_mart.listings` l USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics_all` s USING (user_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;

-- SELLER TIER
SELECT
seller_tier,
CASE WHEN listing_id IN (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings`) THEN 1 ELSE 0 END AS sus_listing,
avg(conversion_rate) as avg_conversion_rate
FROM `etsy-data-warehouse-dev.nlao.listing_cr` 
JOIN `etsy-data-warehouse-prod.listing_mart.listings` l USING (listing_id)
JOIN `etsy-data-warehouse-prod.rollups.seller_basics_all` s USING (user_id)
GROUP BY 1,2
ORDER BY 2 ASC, 1 ASC
;


