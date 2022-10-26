-- This SQL files are the code to get metrics shown in Coverage of High Quality Listings on Etsy (https://docs.google.com/presentation/d/1tqszbvFVzl46fAlnXUm4wFjklPjpui0Y_277SE0oXZg/edit#slide=id.g173525d2c0d_3_0)
-- To run this code, ask nlao@etsy.com to get access to `etsy-data-warehouse-dev.nlao.quality_model_output`

-- CURRENT STASH LISTING /GMS COVERAGE 
WITH stash_listing AS (
SELECT 
  DISTINCT listing_id
FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments`
WHERE concept.type = 'is_stash' 
  AND concept.value = '1'
)
SELECT
  COUNT(listing_id) AS listing_count
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN listing_id ELSE NULL END) AS stash_listing_count
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN listing_id ELSE NULL END) / COUNT(listing_id) AS stash_listing_share
  ,SUM(past_year_gms) AS gms
  ,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN past_year_gms ELSE NULL END) AS stash_listing_gms
  ,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN past_year_gms ELSE NULL END) / SUM(past_year_gms) AS stash_listing_gms_share
  ,COUNT(DISTINCT user_id) AS seller_count
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN user_id ELSE NULL END) AS stash_seller_count
  ,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN user_id ELSE NULL END) / COUNT(DISTINCT user_id) AS stash_seller_share
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics`
;

------------------------------
-- Topline Coverage & Stats --
------------------------------

-- GMS AND LISTING COVERAGE
WITH transactions AS (
SELECT
	listing_id
	,SUM(gms_net) AS gms_net
FROM `etsy-data-warehouse-prod`.transaction_mart.all_transactions t
INNER JOIN `etsy-data-warehouse-prod`.transaction_mart.transactions_gms tt
	ON t.transaction_id = tt.transaction_id
WHERE t.date >= CURRENT_DATE - 90
	AND tt.trans_date >= CURRENT_DATE - 90
GROUP BY 1
)
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
    ELSE "4very_high"
  END AS quality_score_bucket
	,COUNT(*) AS listing_count
	,ROUND(COUNT(*)/SUM(COUNT(*)) OVER(),4) AS share_active_listings
	,ROUND(SUM(gms_net)/SUM(SUM(gms_net)) OVER(),4) AS share_gms
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	LEFT JOIN transactions t
		ON q.listing_id = t.listing_id
GROUP BY 1
ORDER BY 1
;

-- LISTING COVERAGE BY TOP/SECOND CATEGORY
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
    ELSE "4very_high"
  END AS quality_score_bucket
	,top_level_cat_new
	,second_level_cat_new
	,ROUND(COUNT(*)/SUM(COUNT(*)) OVER(),4) AS share_active_listings
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.materialized.listing_categories_taxonomy l
	ON q.listing_id = l.listing_id
GROUP BY 1,2,3
;

-- CONVERSION RATE AND LISTING VIEWS
WITH
listing_views AS (
SELECT
	listing_id
	,COUNT(*) AS lvs
	,SUM(purchased_in_visit) AS piv
	,SUM(favorited) AS favorited
FROM `etsy-data-warehouse-prod`.analytics.listing_views
WHERE
	_date >= CURRENT_DATE - 90
	AND DATE(TIMESTAMP_SECONDS(run_date)) >= CURRENT_DATE - 90
GROUP BY 1
)
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,COUNT(*)
	,ROUND(SUM(lvs)/SUM(SUM(lvs)) OVER(),4) AS listing_view_share
	,ROUND(SUM(piv)/SUM(lvs),4) AS conversion_rate
	,ROUND(SUM(favorited)/SUM(lvs),4) AS collection_rate
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
LEFT JOIN listing_views t
	ON q.listing_id = t.listing_id
GROUP BY 1
ORDER BY 1
;

-- PRICE COMPARISON (MEDIAN AND MEAN)
WITH taxo_price AS(
SELECT
	DISTINCT taxonomy_id
	,AVG(price_usd) OVER (PARTITION BY taxonomy_id) AS avg_price
	,PERCENTILE_CONT(price_usd, 0.5) OVER (PARTITION BY taxonomy_id) AS median_price
FROM 
	`etsy-data-warehouse-prod`.rollups.active_listing_basics
)
,compare AS (
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
    ELSE "4very_high"
  END AS quality_score_bucket
	,a.top_category
	,q.listing_id
	,a.taxonomy_id
	,a.price_usd
	,t.avg_price
	,t.median_price
	,CASE 
    WHEN a.price_usd > t.avg_price THEN 1 
		ELSE 0 
	END AS higher_avg_price_flag
	,CASE 
		WHEN a.price_usd > t.median_price THEN 1 
	  ELSE 0 
	END AS higher_med_price_flag
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	INNER JOIN `etsy-data-warehouse-prod`.rollups.active_listing_basics a
		ON q.listing_id = a.listing_id
	INNER JOIN taxo_price t
		ON a.taxonomy_id = t.taxonomy_id
)
SELECT 
	quality_score_bucket
	,top_category
	,higher_avg_price_flag
	,higher_med_price_flag
	,COUNT(listing_id) AS listing_count
FROM compare
GROUP BY 1,2,3,4
;

-----------------------------------
-- Page & Feature Level Coverage --
-----------------------------------

-- RECS DELIVERED BY MODULE
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
  ,l.module_placement
	,m.module_label
	,COUNT(*) AS share_recs_delivery
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod.analytics.recsys_delivered_listings` l
  ON q.listing_id = l.listing_id
LEFT JOIN `etsy-data-warehouse-prod.static.recsys_module_mapping` m
	ON m.module_placement = l.module_placement
WHERE l._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE 
GROUP BY 1,2,3
;

-- SEARCH QUERY IMPRESSIONS (OVERALL & FIRST PAGE) BY QUERY BIN
-- **`etsy-data-warehouse-prod`.search.visit_level_listing_impressions have search impression data in the past 60 days (web only)

WITH search_impression AS(
SELECT
	listing_id
	,bin
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod.search.visit_level_listing_impressions` v
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` b
	ON v.query = b.query	
WHERE
		_date >= CURRENT_DATE - 60
GROUP BY 1,2
)
SELECT 
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,bin
	,SUM(search_impression_count) AS search_impression_count
	,SUM(first_page_impression_count) AS first_page_impression_count
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN search_impression t
		ON q.listing_id = t.listing_id
GROUP BY 1,2
;

-- SEARCH QUERY IMPRESSIONS (OVERALL & FIRST PAGE) BY QUERY VERTICALS

WITH search_impression AS(
SELECT
	listing_id
	,is_gift
	,is_digital
	,is_holiday
	,is_occasion
	,is_color
	,is_material
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod.search.visit_level_listing_impressions` v
JOIN `etsy-data-warehouse-prod.rollups.query_level_metrics` b
	ON v.query = b.query_normalized	
WHERE
		_date >= CURRENT_DATE - 60
GROUP BY 1,2,3,4,5,6,7
)
SELECT 
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,is_gift
	,is_digital
	,is_holiday
	,is_occasion
	,is_color
	,is_material
	,SUM(search_impression_count) AS search_impression_count
	,SUM(first_page_impression_count) AS first_page_impression_count
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN search_impression t
		ON q.listing_id = t.listing_id
GROUP BY 1,2,3,4,5,6,7
;

--SEARCH IMPRESSION BY TOP VOLUME

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev`.nlao.query_volume AS (
  SELECT
  query
  ,COUNT(*) AS volume
  ,RANK() OVER (ORDER BY COUNT(*) DESC) AS volume_ranking
FROM `etsy-data-warehouse-prod`.search.visit_level_listing_impressions v
WHERE _date >= CURRENT_DATE - 365 AND query != '""'
GROUP BY 1
)
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.query_gms` AS (
  SELECT
  query_raw,
  SUM(gms) AS gms
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
GROUP BY 1
);

SELECT
	CASE WHEN volume_ranking <= 10 THEN 1 ELSE 0 END AS top10_volume
	,CASE WHEN volume_ranking <= 100 THEN 1 ELSE 0 END AS top100_volume
	,CASE WHEN volume_ranking <= 1000 THEN 1 ELSE 0 END AS top1000_volume
	,CASE WHEN volume_ranking <= 10000 THEN 1 ELSE 0 END AS top10000_volume
	,CASE WHEN volume_ranking <= 100000 THEN 1 ELSE 0 END AS top100000_volume
	,CASE WHEN volume_ranking <= 1000000 THEN 1 ELSE 0 END AS top1000000_volume
	,CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod`.search.visit_level_listing_impressions v
JOIN `etsy-data-warehouse-dev`.nlao.query_volume q
	ON v.query = q.query
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output m
	ON v.listing_id = m.listing_id
WHERE
		_date >= CURRENT_DATE - 60
		AND volume_ranking <= 1000000
GROUP BY 1,2,3,4,5,6,7
;

-- TOP 10 VOLUME QUERIES
SELECT 
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
FROM `etsy-data-warehouse-dev.nlao.query_gms` g
LEFT JOIN `etsy-data-warehouse-dev`.nlao.query_volume v
	ON g.query_raw = v.query
;


-- HIGH QUALITY LISTING FAVORITE COUNT DISTRIBUTION
WITH eligible AS (
SELECT 
  collection_id
  ,listing_id
  ,CASE WHEN quality_score >= 0.6 THEN 1 ELSE 0 END AS high_qual_flag
  ,COUNT(*) OVER (PARTITION BY collection_id) AS total_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score >= 0.6 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),0) AS total_high_qual_listing_count
 FROM `etsy-data-warehouse-prod.etsy_shard.collection_listing` 
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output
USING (listing_id)
 WHERE status = 1 --active album
 AND state = 0 --active listings
)
SELECT
CASE WHEN total_high_qual_listing_count <20 THEN CAST(total_high_qual_listing_count AS STRING) ELSE '20+' END AS total_high_qual_listing_count,
COUNT(DISTINCT collection_id) AS total_collection_count,
FROM eligible
WHERE total_listing_count >= 4
GROUP BY 1
;

-- FAVORITE COUNT
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,CASE 
		WHEN favorite_count < 1 THEN '0'
    WHEN favorite_count <= 10 THEN '01-10'
		WHEN favorite_count <= 20 THEN '11-20'
		WHEN favorite_count <= 30 THEN '21-30'
		WHEN favorite_count <= 40 THEN '31-40'
		WHEN favorite_count <= 50 THEN '41-50'
		ELSE '50+'
	END AS favorite_count
	,count(q.listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN
 `etsy-data-warehouse-prod`.listing_mart.listing_counts l
ON q.listing_id = l.listing_id
GROUP BY 1,2
;

--------------------------
-- Post-Purchase Trends --
--------------------------

-- FULFILLMENT: HAVE EDD INFO
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,has_complete_edd
	,COUNT(*) AS listing_count
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN `etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs t
		ON q.listing_id = t.listing_id
GROUP BY 1,2
ORDER BY 1,2
;

-- FULFILLMENT: ONTIME DELIVERY RATE
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,CASE WHEN delivered_date <= initial_edd_max THEN 1 
			 WHEN delivered_date > initial_edd_max THEN 0
			 ELSE NULL 
	END AS ontime_flag
	,COUNT(q.listing_id) AS listing_count
FROM 
  `etsy-data-warehouse-prod`.rollups.receipt_shipping_basics s
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
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
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
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- ACCEPT RETURN & EXCHANGE (LISTING LEVEL)
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,s.accepts_exchanges
	,s.accepts_returns
	,COUNT(q.listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.active_listing_shipping_costs s
		ON q.listing_id = s.listing_id
GROUP BY 1,2,3
ORDER BY 1,2,3
;

-- % OF LISTING WITH A REVIEW (ALL TIME)
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,
	CASE WHEN listing_id IN (
			SELECT listing_id 
			FROM `etsy-data-warehouse-prod`.rollups.transaction_reviews 
			WHERE rating IS NOT NULL
		) 
		THEN 1 
		ELSE 0 
	END AS review_flag
	,COUNT(DISTINCT listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
GROUP BY 1,2
ORDER BY 1,2
;

-- % OF TRANSACTION REVIEWED/TEXT REVIEW/ PHOTO REVIEW
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,SUM(has_review) AS review_count
	,COUNT(DISTINCT q.listing_id) AS listing_count
	,AVG(rating) AS avg_rating
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.transaction_reviews l
		ON q.listing_id = l.listing_id
WHERE DATE(transaction_date) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
;

-- REVIEW RATING DISTRIBUTION
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,rating
	,COUNT(*) AS review_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.transaction_reviews l
		ON q.listing_id = l.listing_id
WHERE DATE(transaction_date) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE 
			AND rating IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
;

-- CREATE A TABLE WITH PURCHASE DATA AND REPURCHASE FLAG
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.purchase` as (
  SELECT DISTINCT
    a.date,
    a.mapped_user_id,
    a.listing_id,
    a.seller_user_id,
    CASE
      WHEN quality_score IS NULL THEN 'unknown'
      WHEN quality_score < 0.3 THEN "1low"
      WHEN quality_score < 0.6 THEN "2mid"
      WHEN quality_score < 0.8 THEN "3high"
      ELSE "4very_high"
    END AS quality_score_bucket
    ,COUNT(a.mapped_user_id) OVER (PARTITION BY a.mapped_user_id ORDER BY UNIX_DATE(DATE(date)) RANGE BETWEEN 1 FOLLOWING AND 90 FOLLOWING) as next_purchase_count
    ,COUNT(a.mapped_user_id) OVER (PARTITION BY a.mapped_user_id, a.seller_user_id ORDER BY UNIX_DATE(DATE(date)) RANGE BETWEEN 1 FOLLOWING AND 90 FOLLOWING) as next_purhcase_same_shop_count
    ,COUNT(a.mapped_user_id) OVER (PARTITION BY a.mapped_user_id, a.listing_id ORDER BY UNIX_DATE(DATE(date)) RANGE BETWEEN 1 FOLLOWING AND 90 FOLLOWING) as next_purhcase_same_listing_count
  FROM `etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
  JOIN `etsy-data-warehouse-prod`.user_mart.user_profile d
    ON a.buyer_user_id = d.user_id
  LEFT JOIN `etsy-data-warehouse-dev.nlao.quality_model_output` q
    ON a.listing_id = q.listing_id
  WHERE a.date >= current_date - 365 - 90
      AND d.is_seller = 0
  ORDER BY 2,1
); 

-- REPURCHASE RATE
SELECT
  quality_score_bucket
  ,COUNT(DISTINCT CASE WHEN next_purchase_count > 0 THEN mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_etsy
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_shop_count > 0 THEN mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_shop
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_listing_count > 0 THEN mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_listing
from `etsy-data-warehouse-dev.nlao.purchase`
WHERE DATE <= CURRENT_DATE - 90 --give a 90 days window for next purchase
GROUP BY 1
;

-- REPURCHASE RATE FOR USERS WHO MADE FIRST PURCHASE IN THE PAST 15 MONTHS
SELECT
  quality_score_bucket
  ,COUNT(DISTINCT CASE WHEN next_purchase_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_etsy
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_shop_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_shop
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_listing_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_listing
FROM `etsy-data-warehouse-dev.nlao.purchase` p
JOIN `etsy-data-warehouse-prod.rollups.buyer_basics` b
  ON p.mapped_user_id = b.mapped_user_id AND p.date = b.first_purchase_date
where DATE <= current_date - 90 
GROUP BY 1
;

-- REPURCHASE RATE BY BUYER SEGMENT
SELECT
  quality_score_bucket
  ,buyer_segment
  ,COUNT(DISTINCT CASE WHEN next_purchase_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_etsy
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_shop_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_shop
  ,COUNT(DISTINCT CASE WHEN next_purhcase_same_listing_count > 0 THEN p.mapped_user_id END) / COUNT(DISTINCT mapped_user_id) AS repeat_purchcase_rate_listing
FROM `etsy-data-warehouse-dev.nlao.purchase` p
JOIN `etsy-data-warehouse-prod.rollups.buyer_basics` b
  ON p.mapped_user_id = b.mapped_user_id
where DATE <= current_date - 90 
GROUP BY 1,2
;

-------------------
-- Seller Trends --
-------------------

-- HIGH QUALITY SELLER TWO DEFINITION:
    -- Inclusive: One or more listings from the seller are high or very high quality
    -- Strict: All listings from the seller are high or very high quality

-- HIGH QUALITY LISTING CONCENTRATION
WITH high_qual_listing AS(
SELECT
l.user_id
,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) AS high_qual_listing_count 
,COUNT(DISTINCT l.listing_id) as active_listing_count
,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) / COUNT(DISTINCT l.listing_id) AS high_qual_concentration
FROM `etsy-data-warehouse-prod`.rollups.active_listing_basics l
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = l.listing_id
GROUP BY 1
)
select
CASE WHEN high_qual_concentration < 0.2 THEN '0-19'
		WHEN high_qual_concentration < 0.4 THEN '20-39'
		WHEN high_qual_concentration < 0.6 THEN '40-59'
		WHEN high_qual_concentration < 0.8 THEN '60-79'
		WHEN high_qual_concentration < 1.0 THEN '80-99'
		WHEN high_qual_concentration = 1.0 THEN '100'
		ELSE cast(high_qual_concentration as string)
		END AS high_qual_concentration
,count(user_id) AS user_count
FROM high_qual_listing
where high_qual_listing_count > 0
GROUP BY 1
ORDER BY 1 ASC
;

   
-- COVERAGE AND STRIBUTION BY SELLER TIER / SELLER COUNTRY / SHOP OPEN YEAR
WITH high_qual_listing AS(
SELECT
	l.user_id
	,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) AS high_qual_listing_count 
	,COUNT(DISTINCT l.listing_id) as active_listing_count
	,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) / COUNT(DISTINCT l.listing_id) AS high_qual_concentration
FROM `etsy-data-warehouse-prod`.rollups.active_listing_basics l
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = l.listing_id
GROUP BY 1
)
SELECT 
	seller_tier
	,CASE WHEN s.country_name IN ('United States','United Kingdom','Canada','France','Germany','Australia','India') THEN s.country_name 
		ELSE 'ROW' END AS country_name
	,CASE WHEN CAST(EXTRACT(year FROM s.open_date) AS INT64) <= 2016 THEN '2016 OR BEFORE'
		ELSE CAST(EXTRACT(year FROM s.open_date) AS STRING) END AS open_year
	,COUNT(DISTINCT user_id) as active_seller_count
	,COUNT(DISTINCT CASE WHEN high_qual_listing_count > 0 THEN user_id ELSE NULL END) AS inclusive_def
	,COUNT(DISTINCT CASE WHEN high_qual_concentration = 1 THEN user_id ELSE NULL END) AS strict_def
FROM high_qual_listing
JOIN `etsy-data-warehouse-prod`.rollups.seller_basics s USING (user_id)
WHERE active_listing_count > 0
GROUP BY 1,2,3
;



