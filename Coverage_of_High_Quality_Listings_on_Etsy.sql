- This SQL files are the code to get metrics shown in Coverage of High Quality Listings on Etsy (https://docs.google.com/presentation/d/1tqszbvFVzl46fAlnXUm4wFjklPjpui0Y_277SE0oXZg/edit#slide=id.g173525d2c0d_3_0)
-- To run this code, ask nlao@etsy.com to get access to `etsy-data-warehouse-dev.nlao.quality_model_output`

-- CURRENT STASH LISTING /GMS COVERAGE 

WITH stash_listing AS (
SELECT 
  DISTINCT listing_id
@@ -27,6 +28,7 @@ FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
------------------------------

-- GMS AND LISTING COVERAGE

WITH transactions AS (
SELECT
	listing_id
@@ -57,6 +59,7 @@ ORDER BY 1
;

-- LISTING COVERAGE BY TOP/SECOND CATEGORY

SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
@@ -75,6 +78,7 @@ GROUP BY 1,2,3
;

-- CONVERSION RATE AND LISTING VIEWS

WITH
listing_views AS (
SELECT
@@ -108,6 +112,7 @@ ORDER BY 1
;

-- PRICE COMPARISON (MEDIAN AND MEAN)

WITH taxo_price AS(
SELECT
	DISTINCT taxonomy_id
@@ -160,6 +165,7 @@ GROUP BY 1,2,3,4
-----------------------------------

-- RECS DELIVERED BY MODULE

SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
@@ -303,21 +309,21 @@ GROUP BY 1,2,3,4,5,6,7

-- TOP 10 VOLUME QUERIES
SELECT 
query
	query
FROM `etsy-data-warehouse-dev`.nlao.query_volume
WHERE volume_ranking <= 10
ORDER BY volume_ranking ASC
;

--TOP VOLUME QUERIES GMS SHARE
SELECT
SUM(gms) AS search_gms
,SUM(CASE WHEN volume_ranking <= 10 THEN gms ELSE NULL END) AS top10_gms
,SUM(CASE WHEN volume_ranking <= 100 THEN gms ELSE NULL END) AS top100_gms
,SUM(CASE WHEN volume_ranking <= 1000 THEN gms ELSE NULL END) AS top1000_gms
,SUM(CASE WHEN volume_ranking <= 10000 THEN gms ELSE NULL END) AS top10000_gms
,SUM(CASE WHEN volume_ranking <= 100000 THEN gms ELSE NULL END) AS top100000_gms
,SUM(CASE WHEN volume_ranking <= 1000000 THEN gms ELSE NULL END) AS top1000000_gms
	SUM(gms) AS search_gms
	,SUM(CASE WHEN volume_ranking <= 10 THEN gms ELSE NULL END) AS top10_gms
	,SUM(CASE WHEN volume_ranking <= 100 THEN gms ELSE NULL END) AS top100_gms
	,SUM(CASE WHEN volume_ranking <= 1000 THEN gms ELSE NULL END) AS top1000_gms
	,SUM(CASE WHEN volume_ranking <= 10000 THEN gms ELSE NULL END) AS top10000_gms
	,SUM(CASE WHEN volume_ranking <= 100000 THEN gms ELSE NULL END) AS top100000_gms
	,SUM(CASE WHEN volume_ranking <= 1000000 THEN gms ELSE NULL END) AS top1000000_gms
FROM `etsy-data-warehouse-dev.nlao.query_gms` g
LEFT JOIN `etsy-data-warehouse-dev`.nlao.query_volume v
	ON g.query_raw = v.query
@@ -339,8 +345,8 @@ USING (listing_id)
 AND state = 0 --active listings
)
SELECT
CASE WHEN total_high_qual_listing_count <20 THEN CAST(total_high_qual_listing_count AS STRING) ELSE '20+' END AS total_high_qual_listing_count,
COUNT(DISTINCT collection_id) AS total_collection_count,
	CASE WHEN total_high_qual_listing_count <20 THEN CAST(total_high_qual_listing_count AS STRING) ELSE '20+' END AS total_high_qual_listing_count
	,COUNT(DISTINCT collection_id) AS total_collection_count
FROM eligible
WHERE total_listing_count >= 4
GROUP BY 1
@@ -364,11 +370,9 @@ SELECT
		ELSE '50+'
	END AS favorite_count
	,count(q.listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN
 `etsy-data-warehouse-prod`.listing_mart.listing_counts l
ON q.listing_id = l.listing_id
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.listing_mart.listing_counts l
	ON q.listing_id = l.listing_id
GROUP BY 1,2
;

@@ -377,6 +381,7 @@ GROUP BY 1,2
--------------------------

-- FULFILLMENT: HAVE EDD INFO

SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
@@ -386,10 +391,9 @@ SELECT
	END AS qual_bucket
	,has_complete_edd
	,COUNT(*) AS listing_count
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN `etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs t
		ON q.listing_id = t.listing_id
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs t
	ON q.listing_id = t.listing_id
GROUP BY 1,2
ORDER BY 1,2
;
@@ -409,16 +413,17 @@ SELECT
	,COUNT(q.listing_id) AS listing_count
FROM 
  `etsy-data-warehouse-prod`.rollups.receipt_shipping_basics s
	JOIN `etsy-data-warehouse-prod`.transaction_mart.all_transactions t
		ON s.receipt_id = t.receipt_id
	JOIN 	`etsy-data-warehouse-dev`.nlao.quality_model_output q
		ON q.listing_id = t.listing_id
JOIN `etsy-data-warehouse-prod`.transaction_mart.all_transactions t
	ON s.receipt_id = t.receipt_id
JOIN 	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = t.listing_id
WHERE	t.date BETWEEN CURRENT_DATE -180 AND CURRENT_DATE - 90
GROUP BY 1,2
ORDER BY 1,2
;

-- ACCEPT RETURN & EXCHANGE (SHOP LEVEL)

SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
@@ -429,13 +434,11 @@ SELECT
	,s.return_policy_type
	,s.accepts_returns
	,COUNT(s.user_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.active_listing_basics l
		ON q.listing_id = l.listing_id
	JOIN `etsy-data-warehouse-prod`.rollups.seller_basics s
		ON l.user_id = s.user_id
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.active_listing_basics l
	ON q.listing_id = l.listing_id
JOIN `etsy-data-warehouse-prod`.rollups.seller_basics s
	ON l.user_id = s.user_id
GROUP BY 1,2,3
ORDER BY 1,2,3
;
@@ -451,10 +454,9 @@ SELECT
	,s.accepts_exchanges
	,s.accepts_returns
	,COUNT(q.listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs s
		ON q.listing_id = s.listing_id
	ON q.listing_id = s.listing_id
GROUP BY 1,2,3
ORDER BY 1,2,3
;
@@ -477,8 +479,7 @@ SELECT
		ELSE 0 
	END AS review_flag
	,COUNT(DISTINCT listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
GROUP BY 1,2
ORDER BY 1,2
;
@@ -494,11 +495,9 @@ SELECT
	,SUM(has_review) AS review_count
	,COUNT(DISTINCT q.listing_id) AS listing_count
	,AVG(rating) AS avg_rating
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.transaction_reviews l
		ON q.listing_id = l.listing_id
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.transaction_reviews l
	ON q.listing_id = l.listing_id
WHERE DATE(transaction_date) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
@@ -514,11 +513,9 @@ SELECT
	END AS qual_bucket
	,rating
	,COUNT(*) AS review_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.transaction_reviews l
		ON q.listing_id = l.listing_id
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.transaction_reviews l
	ON q.listing_id = l.listing_id
WHERE DATE(transaction_date) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE 
			AND rating IS NOT NULL
GROUP BY 1,2
@@ -586,7 +583,7 @@ SELECT
FROM `etsy-data-warehouse-dev.nlao.purchase` p
JOIN `etsy-data-warehouse-prod.rollups.buyer_basics` b
  ON p.mapped_user_id = b.mapped_user_id
where DATE <= current_date - 90 
WHERE DATE <= current_date - 90 
GROUP BY 1,2
;

@@ -621,7 +618,7 @@ CASE WHEN high_qual_concentration < 0.2 THEN '0-19'
		END AS high_qual_concentration
,count(user_id) AS user_count
FROM high_qual_listing
where high_qual_listing_count > 0
WHERE high_qual_listing_count > 0
GROUP BY 1
ORDER BY 1 ASC
;
