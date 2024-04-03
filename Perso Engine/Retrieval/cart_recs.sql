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

-- MODULE ENGAGED PRICE DIFF VS CONVERSION
WITH price_compare AS (
SELECT DISTINCT
r.visit_id,
v.converted,
r.clicked,
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
AND r.platform = "desktop"
)
SELECT  
price_diff,
clicked,
converted,
COUNT(distinct visit_id) AS visits
FROM price_compare
GROUP BY 1,2,3
ORDER BY 1
;


