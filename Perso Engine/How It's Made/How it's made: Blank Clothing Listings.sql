-- listing count and gms
SELECT
COUNT(listing_id) AS n_listing_in_top_category,
COUNT(CASE WHEN top_category IN ("clothing") THEN listing_id ELSE NULL END) AS n_clothing_listing,
COUNT(CASE WHEN top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0 THEN listing_id ELSE NULL END) AS n_blank_listing_in_clothing,
SUM(past_year_gms) AS gms,
SUM(CASE WHEN top_category IN ("clothing") THEN past_year_gms ELSE NULL END) AS gms_clothing_listing,
SUM(CASE WHEN top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0 THEN past_year_gms ELSE NULL END) AS gms_blank_listing_in_clothing,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.materialized.listing_marketplaces` USING (listing_id)
;

-- listing view
SELECT
COUNT(*) AS n_listing_view,
COUNT(CASE WHEN top_category IN ("clothing") THEN listing_id ELSE NULL END) AS n_listing_view_clothing_listing,
COUNT(CASE WHEN top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0 THEN listing_id ELSE NULL END) AS n_listing_view_blank_listing_in_clothing,
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
JOIN `etsy-data-warehouse-prod.analytics.listing_views` USING (listing_id)
JOIN `etsy-data-warehouse-prod.materialized.listing_marketplaces` USING (listing_id)
WHERE _date >= CURRENT_DATE - 30
;

-- % of these listings where seller said “someone else made it”
SELECT
c.value,
COUNT(*) AS n_listing
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
JOIN `etsy-data-warehouse-prod.materialized.listing_marketplaces` b 
  ON a.listing_id = b.listing_id
LEFT JOIN (SELECT listing_id,attribute_value as value FROM `etsy-data-warehouse-prod.listing_mart.listing_all_attributes` WHERE attribute_id = 175271657) c
        ON a.listing_id = c.listing_id
WHERE top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0
GROUP BY 1
ORDER BY 2 DESC
;

-- % of these listings that would get each of the different variant b label based on our MVP logic
SELECT
how_its_made_label,
COUNT(*) AS n_listing
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` a
JOIN `etsy-data-warehouse-prod.materialized.listing_marketplaces` b 
  ON a.listing_id = b.listing_id
JOIN `etsy-data-warehouse-dev.nlao.how_its_made_active_listing_label` c
  ON a.listing_id = c.listing_id
WHERE top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0
GROUP BY 1
ORDER BY 2 DESC
;

-- QA sample listings
SELECT
l.listing_id,
l.taxonomy_id,
t.full_path
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` l
JOIN `etsy-data-warehouse-prod.materialized.listing_marketplaces` b 
  ON l.listing_id = b.listing_id
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t USING (taxonomy_id)
WHERE top_category IN ("clothing") AND LOWER(title) like "%blank%" AND is_digital = 0 AND is_vintage = 0
ORDER BY RAND()
LIMIT 200
;
