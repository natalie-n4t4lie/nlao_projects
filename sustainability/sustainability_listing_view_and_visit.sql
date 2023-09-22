##### LISTING VIEW & VISIT #########
-- SUB CATEGORY
SELECT
COUNT(visit_id) AS view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN visit_id ELSE NULL END) AS sub_listing_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_desc = 1) THEN visit_id ELSE NULL END) AS term_in_desc_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_title = 1) THEN visit_id ELSE NULL END) AS term_in_title_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_title + term_in_desc = 2) THEN visit_id ELSE NULL END) AS term_in_both_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_material_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_material_term_view_count,  
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_generic_eco_labels_term = 1) THEN visit_id ELSE NULL END) AS contains_generic_eco_labels_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_lifestyle_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_lifestyle_terms_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_preferred_manufacturing_term = 1) THEN visit_id ELSE NULL END) AS contains_preferred_manufacturing_term_view_count, 
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_misc_sustainability_term = 1) THEN visit_id ELSE NULL END) AS contains_misc_sustainability_term_view_count,
COUNT(DISTINCT visit_id) AS visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN visit_id ELSE NULL END) AS sub_listing_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_desc = 1) THEN visit_id ELSE NULL END) AS term_in_desc_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_title = 1) THEN visit_id ELSE NULL END) AS term_in_title_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE term_in_title + term_in_desc = 2) THEN visit_id ELSE NULL END) AS term_in_both_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_material_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_material_term_visit_count,  
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_generic_eco_labels_term = 1) THEN visit_id ELSE NULL END) AS contains_generic_eco_labels_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_sustainable_lifestyle_term = 1) THEN visit_id ELSE NULL END) AS contains_sustainable_lifestyle_terms_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_preferred_manufacturing_term = 1) THEN visit_id ELSE NULL END) AS contains_preferred_manufacturing_term_visit_count, 
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage` WHERE contains_misc_sustainability_term = 1) THEN visit_id ELSE NULL END) AS contains_misc_sustainability_term_visit_count
FROM `etsy-data-warehouse-prod.analytics.listing_views` a
WHERE a._date between '2022-04-23' and '2022-05-23'
;

-- TOP_CATEGORY 
SELECT
top_category,
COUNT(visit_id) AS view_count,
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN visit_id ELSE NULL END) AS sub_listing_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) THEN visit_id ELSE NULL END) AS sub_listing_visit_count
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
COUNT(CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS sub_perso_view_count,
COUNT(CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS sub_wedding_view_count,
COUNT(CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS sub_gift_view_count,
COUNT(CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS sub_vintage_view_count,
COUNT(DISTINCT CASE WHEN is_personalized = 1 THEN visit_id ELSE NULL END) AS sub_perso_visit_count,
COUNT(DISTINCT CASE WHEN is_wedding_title = 1 THEN visit_id ELSE NULL END) AS sub_wedding_visit_count,
COUNT(DISTINCT CASE WHEN is_gift_title = 1 THEN visit_id ELSE NULL END) AS sub_gift_visit_count,
COUNT(DISTINCT CASE WHEN is_vintage = 1 THEN visit_id ELSE NULL END) AS sub_vintage_visit_count
FROM `etsy-data-warehouse-prod.rollups.transaction_level_horizontal_taxonomy`
JOIN `etsy-data-warehouse-prod.analytics.listing_views` a USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23'
  AND listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`) 
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
    AND listing_id in (select listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`)
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
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`)THEN visit_id ELSE NULL END) AS sub_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`)THEN visit_id ELSE NULL END) AS sub_visit_count,
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
COUNT(CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`)THEN visit_id ELSE NULL END) AS sub_view_count,
COUNT(DISTINCT visit_id) AS visit_count,
COUNT(DISTINCT CASE WHEN listing_id in (SELECT listing_id FROM `etsy-data-warehouse-dev.nlao.sustainability_term_listings_no_vintage`)THEN visit_id ELSE NULL END) AS sub_visit_count,
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
        WHERE term NOT IN ('green','vintage')
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
    COUNT(visit_id) AS count_view,
    COUNT(DISTINCT visit_id) AS count_visit
FROM combined
JOIN `etsy-data-warehouse-prod.analytics.listing_views` a USING (listing_id)
WHERE a._date between '2022-04-23' and '2022-05-23' 
GROUP BY 1
ORDER BY 2 desc
;
