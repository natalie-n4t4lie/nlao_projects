-- HOW MANY PERCENT OF CART PAGE VISIT HAS RECOMMENDATIONS?
WITH cart_recs_delivered AS (
SELECT  
DISTINCT visit_id
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" 
AND _date >= current_date - 30
AND platform = "desktop"
)
SELECT  
COUNT(DISTINCT visit_id) AS cart_page_visit,
COUNT(DISTINCT CASE WHEN visit_id IN (SELECT visit_id FROM cart_recs_delivered) THEN visit_id ELSE NULL END) AS cart_recs_delivered_visits
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id, _date)
WHERE v._date >= current_date - 30
AND v.event_source = "web" AND v.is_mobile_device = 0 -- desktop visit
AND e.event_type = "cart_view"
;

-- MODULE ENGAGEMENT
SELECT  
COUNT(DISTINCT visit_id) AS delivered_visit,
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_visit,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_visit,
COUNT(DISTINCT CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart_visit,
COUNT(DISTINCT CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view_visit,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE _date BETWEEN '2024-01-01' AND '2024-02-29'
AND module_placement = "cart"
AND platform = "desktop"
;

SELECT 
COUNT(DISTINCT visit_id) AS delivered_visit,
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_visit,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_visit,
COUNT(DISTINCT CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart_visit,
COUNT(DISTINCT CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view_visit,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE _date BETWEEN '2024-01-01' AND '2024-02-29'
AND module_page = "view_listing"
AND platform = "desktop"
;

-- MODULE ENGAGEMENT BY BUYER SEGMENT
SELECT  
buyer_segment, 
COUNT(DISTINCT visit_id) AS delivered_visit,
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_visit,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_visit,
COUNT(DISTINCT CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart_visit,
COUNT(DISTINCT CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view_visit,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
GROUP BY 1
ORDER BY 2 DESC
;

-- MODULE ENGAGEMENT BY TARGET AND RECS PRICE DIFF
WITH price_compare AS (
SELECT
visit_id,
listing_id,
seen,
clicked,
added_to_cart,
purchased_after_view,
CASE WHEN rec_price/target_price-1 = 0 THEN "same price"
     WHEN rec_price/target_price-1 < 0 AND rec_price/target_price-1 >= -0.1 THEN "Cheaper <10% "
     WHEN rec_price/target_price-1 < -0.1 AND rec_price/target_price-1 >= -0.2 THEN "Cheaper 11-20%"
     WHEN rec_price/target_price-1 < -0.2 AND rec_price/target_price-1 >= -0.3 THEN "Cheaper 21-30%"
     WHEN rec_price/target_price-1 < -0.3 AND rec_price/target_price-1 >= -0.4 THEN "Cheaper 31-40%"
     WHEN rec_price/target_price-1 < -0.4 AND rec_price/target_price-1 >= -0.5 THEN "Cheaper 41-50%"
     WHEN rec_price/target_price-1 < -0.5 THEN "Cheaper 51%+ "
     WHEN rec_price/target_price-1 > 0 AND rec_price/target_price-1 <= 0.1 THEN "More expensive <10%"
     WHEN rec_price/target_price-1 > 0.1 AND rec_price/target_price-1 <= 0.2 THEN "More expensive 11-20%"
     WHEN rec_price/target_price-1 > 0.2 AND rec_price/target_price-1 <= 0.3 THEN "More expensive 21-30%"
     WHEN rec_price/target_price-1 > 0.3 AND rec_price/target_price-1 <= 0.4 THEN "More expensive 31-40%"
     WHEN rec_price/target_price-1 > 0.4 AND rec_price/target_price-1 <= 0.5 THEN "More expensive 41-50%"
     WHEN rec_price/target_price-1 > 0.5 THEN "More expensive 51%+"
     END AS price_diff
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
)
SELECT  
price_diff,
COUNT(visit_id) AS deliver,
COUNT(CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen,
COUNT(CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked,
COUNT(CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart,
COUNT(CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view
FROM price_compare
GROUP BY 1
ORDER BY 1
;

-- MODULE ENGAGEMENT BY RECS PRICE RANGE
WITH price_compare AS (
SELECT
visit_id,
listing_id,
seen,
clicked,
added_to_cart,
purchased_after_view,
CASE WHEN rec_price BETWEEN 0 AND 10 THEN "1. Below $10"
     WHEN rec_price BETWEEN 10 AND 20 THEN "2. $10-$20"
     WHEN rec_price BETWEEN 20 AND 30 THEN "3. $20-$30"
     WHEN rec_price BETWEEN 30 AND 40 THEN "4. $30-$40"
     WHEN rec_price BETWEEN 40 AND 50 THEN "5. $40-$50"
     WHEN rec_price BETWEEN 50 AND 60 THEN "6. $50-$60"
     WHEN rec_price BETWEEN 60 AND 70 THEN "7. $60-$70"
     WHEN rec_price BETWEEN 70 AND 80 THEN "8. $70-$80"
     WHEN rec_price BETWEEN 80 AND 90 THEN "9. $80-$90"
     WHEN rec_price BETWEEN 90 AND 100 THEN "10. $90-$100"
     WHEN rec_price >100 THEN "11. Above $100+"
     END AS price_range
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
)
SELECT  
price_range,
COUNT(visit_id) AS deliver,
COUNT(CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen,
COUNT(CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked,
COUNT(CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart,
COUNT(CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view
FROM price_compare
GROUP BY 1
ORDER BY 1
;


-- MODULE ENGAGEMENT FOR CANDIDATES THAT ARE IN THE SAME VS DIFF TAXONOMY/TOP CAT AS THE TARGET LISTINGS
SELECT  
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id THEN visit_id ELSE NULL END) AS same_taxo_deliver,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND seen = 1 THEN visit_id ELSE NULL END) AS same_taxo_seen,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND clicked = 1 THEN visit_id ELSE NULL END) AS same_taxo_clicked,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS same_taxo_added_to_cart,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS same_taxo_purchased_after_view,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id THEN visit_id ELSE NULL END) AS diff_taxo_deliver,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND seen = 1 THEN visit_id ELSE NULL END) AS diff_taxo_seen,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND clicked = 1 THEN visit_id ELSE NULL END) AS diff_taxo_clicked,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS diff_taxo_added_to_cart,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS diff_taxo_purchased_after_view,
COUNT(CASE WHEN target_top_category = rec_top_category THEN visit_id ELSE NULL END) AS same_topcat_deliver,
COUNT(CASE WHEN target_top_category = rec_top_category AND seen = 1 THEN visit_id ELSE NULL END) AS same_topcat_seen,
COUNT(CASE WHEN target_top_category = rec_top_category AND clicked = 1 THEN visit_id ELSE NULL END) AS same_topcat_clicked,
COUNT(CASE WHEN target_top_category = rec_top_category AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS same_topcat_added_to_cart,
COUNT(CASE WHEN target_top_category = rec_top_category AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS same_topcat_purchased_after_view,
COUNT(CASE WHEN target_top_category != rec_top_category THEN visit_id ELSE NULL END) AS diff_topcat_deliver,
COUNT(CASE WHEN target_top_category != rec_top_category AND seen = 1 THEN visit_id ELSE NULL END) AS diff_topcat_seen,
COUNT(CASE WHEN target_top_category != rec_top_category AND clicked = 1 THEN visit_id ELSE NULL END) AS diff_topcat_clicked,
COUNT(CASE WHEN target_top_category != rec_top_category AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS diff_topcat_added_to_cart,
COUNT(CASE WHEN target_top_category != rec_top_category AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS diff_topcat_purchased_after_view
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
;

-- MODULE ENGAGEMENT FOR CANDIDATES THAT ARE IN THE SAME VS DIFF TAXONOMY ID AS THE TARGET LISTINGS BY TOP CATEGORY
SELECT  
target_top_category,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id THEN visit_id ELSE NULL END) AS same_taxo_deliver,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND seen = 1 THEN visit_id ELSE NULL END) AS same_taxo_seen,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND clicked = 1 THEN visit_id ELSE NULL END) AS same_taxo_clicked,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS same_taxo_added_to_cart,
COUNT(CASE WHEN target_taxonomy_id = rec_taxonomy_id AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS same_taxo_purchased_after_view,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id THEN visit_id ELSE NULL END) AS diff_taxo_deliver,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND seen = 1 THEN visit_id ELSE NULL END) AS diff_taxo_seen,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND clicked = 1 THEN visit_id ELSE NULL END) AS diff_taxo_clicked,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS diff_taxo_added_to_cart,
COUNT(CASE WHEN target_taxonomy_id != rec_taxonomy_id AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS diff_taxo_purchased_after_view,
COUNT(CASE WHEN target_top_category = rec_top_category THEN visit_id ELSE NULL END) AS same_topcat_deliver,
COUNT(CASE WHEN target_top_category = rec_top_category AND seen = 1 THEN visit_id ELSE NULL END) AS same_topcat_seen,
COUNT(CASE WHEN target_top_category = rec_top_category AND clicked = 1 THEN visit_id ELSE NULL END) AS same_topcat_clicked,
COUNT(CASE WHEN target_top_category = rec_top_category AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS same_topcat_added_to_cart,
COUNT(CASE WHEN target_top_category = rec_top_category AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS same_topcat_purchased_after_view,
COUNT(CASE WHEN target_top_category != rec_top_category THEN visit_id ELSE NULL END) AS diff_topcat_deliver,
COUNT(CASE WHEN target_top_category != rec_top_category AND seen = 1 THEN visit_id ELSE NULL END) AS diff_topcat_seen,
COUNT(CASE WHEN target_top_category != rec_top_category AND clicked = 1 THEN visit_id ELSE NULL END) AS diff_topcat_clicked,
COUNT(CASE WHEN target_top_category != rec_top_category AND added_to_cart = 1 THEN visit_id ELSE NULL END) AS diff_topcat_added_to_cart,
COUNT(CASE WHEN target_top_category != rec_top_category AND purchased_after_view = 1 THEN visit_id ELSE NULL END) AS diff_topcat_purchased_after_view
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
GROUP BY 1
;

-- MODULE ENGAGEMENT BY TARGET TOP CAT
SELECT  
target_top_category, 
COUNT(DISTINCT visit_id) AS delivered_visit,
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_visit,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_visit,
COUNT(DISTINCT CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart_visit,
COUNT(DISTINCT CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view_visit,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
GROUP BY 1
ORDER BY 2 DESC
;

-- CANDIDATE LISTING CATEGORY
SELECT  
rec_top_category, 
COUNT(DISTINCT visit_id) AS delivered_visit,
COUNT(DISTINCT CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen_visit,
COUNT(DISTINCT CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked_visit,
COUNT(DISTINCT CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart_visit,
COUNT(DISTINCT CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view_visit,
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND _date BETWEEN '2024-01-01' AND '2024-02-29'
AND platform = "desktop"
GROUP BY 1
ORDER BY 2 DESC
;
----------------------------------------------------------------------------------------------------------------------------------
-- MODULE ENGAGMENT VS CONVERSION 
WITH cart_recs_clicked AS (
SELECT  
rec_top_category, 
visit_id
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND clicked = 1
AND _date >= current_date - 30
AND platform = "desktop"
)
SELECT  
converted,
CASE WHEN visit_id IN (SELECT visit_id FROM cart_recs_clicked) THEN 1 ELSE 0 END AS clicked_cart_module,
COUNT(DISTINCT visit_id) AS cart_page_visit
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id, _date)
WHERE v._date >= current_date - 30
AND v.event_source = "web" AND v.is_mobile_device = 0 -- desktop visit
AND e.event_type = "cart_view"
GROUP BY 1,2
;

-- CONVERSION AND CATEGORY ENGAGEMENT
WITH cart_recs_clicked AS (
SELECT 
rec_top_category,
visit_id
FROM `etsy-data-warehouse-prod.rollups.recsys_delivered_listings`
WHERE module_placement = "cart" AND clicked = 1
AND _date >= current_date - 30
AND platform = "desktop"
)
SELECT  
rec_top_category,
COUNT(DISTINCT v.visit_id) AS cart_page_visit,
COUNT(DISTINCT CASE WHEN converted = 1 THEN v.visit_id ELSE NULL END) AS cart_page_converted_visit
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.weblog.events` e USING (visit_id, _date)
JOIN cart_recs_clicked c
     ON v.visit_id = c.visit_id
WHERE v._date >= current_date - 30
AND v.event_source = "web" AND v.is_mobile_device = 0 -- desktop visit
AND e.event_type = "cart_view"
GROUP BY 1
;


WITH price_compare AS (
SELECT
r.visit_id,
r.listing_id,
v.converted,
r.seen,
r.clicked,
r.added_to_cart,
r.purchased_after_view,
CASE WHEN rec_price/target_price-1 = 0 THEN "same price"
     WHEN rec_price/target_price-1 < 0 AND rec_price/target_price-1 >= -0.1 THEN "Cheaper <10% "
     WHEN rec_price/target_price-1 < -0.1 AND rec_price/target_price-1 >= -0.2 THEN "Cheaper 11-20%"
     WHEN rec_price/target_price-1 < -0.2 AND rec_price/target_price-1 >= -0.3 THEN "Cheaper 21-30%"
     WHEN rec_price/target_price-1 < -0.3 AND rec_price/target_price-1 >= -0.4 THEN "Cheaper 31-40%"
     WHEN rec_price/target_price-1 < -0.4 AND rec_price/target_price-1 >= -0.5 THEN "Cheaper 41-50%"
     WHEN rec_price/target_price-1 < -0.5 THEN "Cheaper 51%+ "
     WHEN rec_price/target_price-1 > 0 AND rec_price/target_price-1 <= 0.1 THEN "More expensive <10%"
     WHEN rec_price/target_price-1 > 0.1 AND rec_price/target_price-1 <= 0.2 THEN "More expensive 11-20%"
     WHEN rec_price/target_price-1 > 0.2 AND rec_price/target_price-1 <= 0.3 THEN "More expensive 21-30%"
     WHEN rec_price/target_price-1 > 0.3 AND rec_price/target_price-1 <= 0.4 THEN "More expensive 31-40%"
     WHEN rec_price/target_price-1 > 0.4 AND rec_price/target_price-1 <= 0.5 THEN "More expensive 41-50%"
     WHEN rec_price/target_price-1 > 0.5 THEN "More expensive 51%+"
     END AS price_diff
FROM `etsy-data-warehouse-prod.weblog.visits` v
JOIN `etsy-data-warehouse-prod.rollups.recsys_delivered_listings` r
     ON v.visit_id = r.visit_id
WHERE module_placement = "cart" 
AND v._date BETWEEN '2024-01-01' AND '2024-02-29' 
AND r._date BETWEEN '2024-01-01' AND '2024-02-29' 
AND r.clicked = 1
AND r.platform = "desktop"
)
SELECT  
converted,
price_diff,
COUNT(visit_id) AS deliver,
COUNT(CASE WHEN seen = 1 THEN visit_id ELSE NULL END) AS seen,
COUNT(CASE WHEN clicked = 1 THEN visit_id ELSE NULL END) AS clicked,
COUNT(CASE WHEN added_to_cart = 1 THEN visit_id ELSE NULL END) AS added_to_cart,
COUNT(CASE WHEN purchased_after_view = 1 THEN visit_id ELSE NULL END) AS purchased_after_view
FROM price_compare
GROUP BY 1
ORDER BY 1
;


