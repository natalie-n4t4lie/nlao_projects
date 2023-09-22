- This SQL files are the code to get metrics shown in Coverage of High Quality Listings on Etsy (https://docs.google.com/presentation/d/1tqszbvFVzl46fAlnXUm4wFjklPjpui0Y_277SE0oXZg/edit#slide=id.g173525d2c0d_3_0)
-- To run this code, ask nlao@etsy.com to get access to `etsy-data-warehouse-dev.nlao.quality_model_output`

-- Evaluation Metrics

-- GMS AND LISTING COVERAGE
WITH txns AS (
	SELECT
		listing_id
		,SUM(gms_net) AS gms_net
	FROM
		`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
		INNER JOIN `etsy-data-warehouse-prod`.transaction_mart.transactions_gms tt
			ON t.transaction_id = tt.transaction_id
	where
		t.date >= CURRENT_DATE - 90
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
	LEFT JOIN txns t
		ON q.listing_id = t.listing_id
GROUP BY 1
ORDER BY 1
;

-- LISTING COVERAGE BY CATEGORY
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
    ELSE "4very_high"
  END AS quality_score_bucket
	,top_category
	,ROUND(COUNT(*)/SUM(COUNT(*)) OVER(),4) AS share_active_listings
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod`.rollups.active_listing_basics l
	ON q.listing_id = l.listing_id
GROUP BY 1,2
ORDER BY 1,2
;

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



-- MORE LIKELY TO CONVERT?
WITH
listing_views AS (
	SELECT
		listing_id
		,COUNT(*) AS lvs
		,SUM(purchased_in_visit) AS piv
		,SUM(favorited) AS favorited
	FROM
		`etsy-data-warehouse-prod`.analytics.listing_views
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
	,ROUND(SUM(lvs)/SUM(SUM(lvs)) OVER(),4) AS share_lvs
	,ROUND(SUM(piv)/SUM(lvs),4) AS conversion_rate
	,ROUND(SUM(favorited)/SUM(lvs),4) AS collection_rate
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	LEFT JOIN listing_views t
		ON q.listing_id = t.listing_id
GROUP BY 1
ORDER BY 1
;

-- HIGHER PRICED?
SELECT
	DISTINCT top_category
	,AVG(price_usd) OVER(PARTITION BY top_category) AS avg_price
	,PERCENTILE_CONT(price_usd, 0.5) OVER(PARTITION BY top_category) AS median_price
FROM 
	`etsy-data-warehouse-prod`.rollups.active_listing_basics
;

WITH taxo_price AS(
SELECT
	DISTINCT taxonomy_id
	,AVG(price_usd) OVER(PARTITION BY taxonomy_id) AS avg_price
	,PERCENTILE_CONT(price_usd, 0.5) OVER(PARTITION BY taxonomy_id) AS median_price
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

-- SEARCH IMPRESSION
WITH search_first_page_impression AS(
SELECT
	listing_id
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod`.search.visit_level_listing_impressions
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
	,ROUND(SUM(search_impression_count)/SUM(SUM(search_impression_count)) OVER(),4) AS share_search_impression
	,ROUND(SUM(first_page_impression_count)/SUM(SUM(first_page_impression_count)) OVER(),4) AS share_first_page_impression
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	LEFT JOIN search_first_page_impression t
		ON q.listing_id = t.listing_id
GROUP BY 1
ORDER BY 1
;

-- SEARCH QUERY bin
WITH search_first_page_impression AS(
SELECT
	listing_id
	,bin
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod`.search.visit_level_listing_impressions v
JOIN `etsy-data-warehouse-prod`.search.query_bins q
	ON v.query = q.query_raw
WHERE
		_date >= CURRENT_DATE - 90
		AND DATE(TIMESTAMP_SECONDS(run_date)) >= CURRENT_DATE - 90
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
	,ROUND(SUM(search_impression_count)/SUM(SUM(search_impression_count)) OVER(),4) AS share_search_impression
	,ROUND(SUM(first_page_impression_count)/SUM(SUM(first_page_impression_count)) OVER(),4) AS share_first_page_impression
FROM
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	LEFT JOIN search_first_page_impression t
		ON q.listing_id = t.listing_id
GROUP BY 1,2
ORDER BY 1,2
;

--search query type
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
	ON v.query = b.query
WHERE
		_date >= CURRENT_DATE - 90
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

-- HAVE EDD INFO
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

-- ONTIME DELIVERY
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

-- ACCEPT RETURN
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,s.return_policy_type
	,s.accepts_returns
	,COUNT(q.listing_id) AS listing_count
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

-- TRANSACTION REVIEW
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,SUM(has_review) AS review_count
	,SUM(has_text_review) AS text_review_count
	,SUM(has_image) AS image_review_count
	,COUNT(*) AS transaction_count
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

-- REVIEW RATING
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,RATING
	,COUNT(DISTINCT q.listing_id) AS listing_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
	JOIN 
	`etsy-data-warehouse-prod`.rollups.transaction_reviews l
		ON q.listing_id = l.listing_id
WHERE DATE(transaction_date) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE
GROUP BY 1,2
ORDER BY 1,2
;

--having a review or not
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


-- CASE COUNT
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,COUNT(*) AS case_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN 
	`etsy-data-warehouse-prod`.rollups.case_stats c
		ON q.listing_id = c.listing_id 
WHERE 
	DATE(TIMESTAMP_SECONDS(case_date)) BETWEEN CURRENT_DATE - 365 AND CURRENT_DATE
	AND DATE(TIMESTAMP_SECONDS(order_date)) BETWEEN CURRENT_DATE - 455 AND CURRENT_DATE - 90
GROUP BY 1
ORDER BY 1
;

SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,COUNT(*) AS transaction_count
FROM 
	`etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN 
	`etsy-data-warehouse-prod`.transaction_mart.all_transactions t
		ON q.listing_id = t.listing_id 
WHERE date BETWEEN CURRENT_DATE - 455 AND CURRENT_DATE - 90
GROUP BY 1
ORDER BY 1
;

-- RECS DELIVER BY MODULE
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
  ,module_placement
	,COUNT(*) AS share_recs_delivery
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod.analytics.recsys_delivered_listings` l
  ON q.listing_id = l.listing_id
WHERE l._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE 
GROUP BY 1,2
;

--RECS DELIVERY
SELECT
	CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,ROUND(COUNT(*)/SUM(COUNT(*)) OVER(),4) AS share_recs_delivery
FROM `etsy-data-warehouse-dev`.nlao.quality_model_output q
JOIN `etsy-data-warehouse-prod.analytics.recsys_delivered_listings` l
  ON q.listing_id = l.listing_id
WHERE l._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE 
GROUP BY 1
;

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
COUNT(DISTINCT collection_id) AS total_collection_count,
FROM eligible
WHERE total_high_qual_listing_count = total_listing_count
;

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


WITH eligible AS (
SELECT 
  collection_id
  ,listing_id
  ,CASE WHEN quality_score >= 0.6 THEN 1 ELSE 0 END AS high_qual_flag
  ,COUNT(*) OVER (PARTITION BY collection_id) AS total_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score < 0.3 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_low_qual_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score < 0.6 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_low_mid_qual_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score >= 0.3 AND quality_score < 0.6 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_mid_qual_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score >= 0.6 AND quality_score < 0.8 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_high_qual_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score >= 0.6 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_high_vhigh_qual_listing_count
  ,COALESCE(COUNT(DISTINCT CASE WHEN quality_score >= 0.8 THEN listing_id ELSE NULL END) OVER (PARTITION BY collection_id),null) AS total_vhigh_qual_listing_count
 FROM `etsy-data-warehouse-prod.etsy_shard.collection_listing` 
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output
USING (listing_id)
 WHERE status = 1 --active album
 AND state = 0 --active listings
)
SELECT
COUNT(DISTINCT collection_id) AS total_collection_count,
COUNT(DISTINCT CASE WHEN total_low_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_low_qual_count,
COUNT(DISTINCT CASE WHEN total_low_mid_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_lowmid_qual_count,
COUNT(DISTINCT CASE WHEN total_mid_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_mid_qual_count,
COUNT(DISTINCT CASE WHEN total_high_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_high_qual_count,
COUNT(DISTINCT CASE WHEN total_high_vhigh_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_highvhigh_qual_count,
COUNT(DISTINCT CASE WHEN total_vhigh_qual_listing_count = total_listing_count THEN collection_id ELSE NULL END) AS all_vhigh_qual_count,
FROM eligible
;


SELECT
CASE WHEN total_high_qual_listing_count <20 THEN CAST(total_high_qual_listing_count AS STRING) ELSE '20+' END AS total_high_qual_listing_count,
COUNT(DISTINCT collection_id) AS total_collection_count,
FROM eligible
WHERE total_listing_count >= 4
GROUP BY 1
;


create or replace table `etsy-data-warehouse-dev.nlao.purchase` as (
  select distinct
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
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_purchase_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.seller_user_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_purhcase_same_shop_count
    ,count(a.mapped_user_id) over (partition by a.mapped_user_id, a.listing_id order by unix_date(date(date)) RANGE between 1 following and 90 following) as next_purhcase_same_listing_count
  FROM `etsy-data-warehouse-prod`.transaction_mart.all_transactions a 
  JOIN `etsy-data-warehouse-prod`.user_mart.user_profile d
    on a.buyer_user_id = d.user_id
  LEFT JOIN `etsy-data-warehouse-dev.nlao.quality_model_output` q
    on a.listing_id = q.listing_id
  where a.date >= current_date - 365 - 90
      and d.is_seller = 0
  ORDER BY 2,1
); 

select
  quality_score_bucket,
  count(distinct case when next_purchase_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_etsy,
  count(distinct case when next_purhcase_same_shop_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_shop,
  count(distinct case when next_purhcase_same_listing_count > 0 then mapped_user_id end) / count(distinct mapped_user_id) AS repeat_purchcase_rate_listing,
from `etsy-data-warehouse-dev.nlao.purchase`
where DATE <= current_date - 90
GROUP BY 1
;


-- stash listing coverage
WITH stash_listing AS (
SELECT DISTINCT listing_id
FROM `etsy-data-warehouse-prod.knowledge_base.listing_assignments`
WHERE concept.type = 'is_stash' AND concept.value = '1'
)
SELECT
COUNT(al.listing_id) as listing_count
,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.listing_id ELSE NULL END) AS stash_listing_count
,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.listing_id ELSE NULL END) / COUNT(al.listing_id) as stash_listing_share,
SUM(al.past_year_gms) AS gms
,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.past_year_gms ELSE NULL END) AS stash_listing_gms
,SUM(CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.past_year_gms ELSE NULL END) / SUM(al.past_year_gms) as stash_listing_gms_share
,COUNT(DISTINCT user_id) as seller_count
,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.user_id ELSE NULL END)
,COUNT(DISTINCT CASE WHEN listing_id IN (SELECT listing_id FROM stash_listing) THEN al.user_id ELSE NULL END) / COUNT(DISTINCT user_id) as stash_seller_share
FROM `etsy-data-warehouse-prod.rollups.active_listing_basics` al
;

WITH high_qual_seller AS (
SELECT
DISTINCT s.user_id
FROM `etsy-data-warehouse-prod`.rollups.seller_basics s
JOIN 
	`etsy-data-warehouse-prod`.rollups.active_listing_basics l
		ON l.user_id = s.user_id
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = l.listing_id
WHERE quality_score >= 0.6
)
SELECT 
	s.seller_tier
	,CASE WHEN s.country_name IN ('United States','United Kingdom','Canada','France','Germany','Australia','India') THEN s.country_name 
	ELSE 'ROW' END AS country_name
	,EXTRACT(year FROM s.open_date) AS open_year
	,COUNT(s.user_id) as total_seller
	,COUNT(CASE WHEN s.user_id IN (SELECT user_id FROM high_qual_seller) THEN s.user_id ELSE NULL END) as high_quality_seller
FROM `etsy-data-warehouse-prod`.rollups.seller_basics s
WHERE active_listings > 0
GROUP BY 1,2,3
;

WITH high_qual_seller AS (
SELECT
DISTINCT s.user_id
FROM `etsy-data-warehouse-prod`.rollups.seller_basics s
JOIN 
	`etsy-data-warehouse-prod`.rollups.active_listing_basics l
		ON l.user_id = s.user_id
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = l.listing_id
WHERE quality_score >= 0.6
),
prolist_seller AS (
SELECT
	shop_id,
	p.active_budget_days_30d,
	p.spend_30d
FROM `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` p
WHERE date = current_date - 1
)
SELECT 
	CASE WHEN spend_30d IS NULL THEN '$0'
	 WHEN spend_30d < 25 THEN '< $25'
	 WHEN spend_30d <50 THEN '< $50'
	 WHEN spend_30d <100 THEN '< $100' 
	 WHEN spend_30d <200 THEN '< $200' 
	 WHEN spend_30d <300 THEN '< $300' 
	 WHEN spend_30d <400 THEN '< $400' 
	 WHEN spend_30d <500 THEN '< $500'
   WHEN spend_30d >= 500 THEN '$500+' 
	 END AS budget_bucket
	,COUNT(s.user_id) as total_seller
	,COUNT(CASE WHEN s.user_id IN (SELECT user_id FROM high_qual_seller) THEN s.user_id ELSE NULL END) as high_quality_seller
FROM `etsy-data-warehouse-prod`.rollups.seller_basics s
LEFT JOIN prolist_seller USING (shop_id)
WHERE active_listings > 0
GROUP BY 1
;

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


--two high quality seller def
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
GROUP BY 1,2,3
;

-- two high quality seller def coverage
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
COUNT(DISTINCT user_id) as active_seller_count,
COUNT(DISTINCT CASE WHEN high_qual_listing_count > 0 THEN user_id ELSE NULL END) AS inclusive_def,
COUNT(DISTINCT CASE WHEN high_qual_concentration = 1 THEN user_id ELSE NULL END) AS strict_def
FROM high_qual_listing
;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.top_gms_query` AS (
  SELECT
  query_normalized,
  SUM(gms) AS gms,
  RANK() OVER (ORDER BY SUM(gms) DESC) AS gms_order
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
GROUP BY 1
);

SELECT
SUM(gms) AS search_gms
,SUM(CASE WHEN gms_order <= 10 THEN gms ELSE NULL END) AS top10_gms
,SUM(CASE WHEN gms_order <= 100 THEN gms ELSE NULL END) AS top100_gms
,SUM(CASE WHEN gms_order <= 1000 THEN gms ELSE NULL END) AS top1000_gms
,SUM(CASE WHEN gms_order <= 10000 THEN gms ELSE NULL END) AS top10000_gms
,SUM(CASE WHEN gms_order <= 100000 THEN gms ELSE NULL END) AS top100000_gms
,SUM(CASE WHEN gms_order <= 1000000 THEN gms ELSE NULL END) AS top1000000_gms
FROM `etsy-data-warehouse-dev.nlao.top_gms_query`
;

SELECT
	CASE WHEN gms_order <= 10 THEN 1 ELSE 0 END AS top10_gms
	,CASE WHEN gms_order <= 100 THEN 1 ELSE 0 END AS top100_gms
	,CASE WHEN gms_order <= 1000 THEN 1 ELSE 0 END AS top1000_gms
	,CASE WHEN gms_order <= 10000 THEN 1 ELSE 0 END AS top10000_gms
	,CASE WHEN gms_order <= 100000 THEN 1 ELSE 0 END AS top100000_gms
	,CASE WHEN gms_order <= 1000000 THEN 1 ELSE 0 END AS top1000000_gms
	,CASE
		WHEN quality_score < 0.3 THEN "1low"
		WHEN quality_score < 0.6 THEN "2mid"
		WHEN quality_score < 0.8 THEN "3high"
		ELSE "4very_high"
	END AS qual_bucket
	,SUM(impressions) AS search_impression_count
	,SUM(CASE WHEN page_no = 1 THEN impressions ELSE 0 END) AS first_page_impression_count
FROM `etsy-data-warehouse-prod`.search.visit_level_listing_impressions v
JOIN `etsy-data-warehouse-dev.nlao.top_gms_query` q
	ON v.query = q.query_normalized
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output m
	ON v.listing_id = m.listing_id
WHERE
		_date >= CURRENT_DATE - 90
		AND DATE(TIMESTAMP_SECONDS(run_date)) >= CURRENT_DATE - 90
		AND gms_order <= 1000000
GROUP BY 1,2,3,4,5,6,7
;

--AD SPEND
WITH seller_counts AS(
SELECT
	l.user_id
	,l.shop_id
	,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) AS high_qual_listing_count 
	,COUNT(DISTINCT l.listing_id) as active_listing_count
	,COUNT(CASE WHEN quality_score >= 0.6 then l.listing_id else null end) / COUNT(DISTINCT l.listing_id) AS high_qual_concentration
FROM `etsy-data-warehouse-prod`.rollups.active_listing_basics l
JOIN `etsy-data-warehouse-dev`.nlao.quality_model_output q
	ON q.listing_id = l.listing_id
GROUP BY 1,2
),
qual_def AS (
SELECT 
	user_id
	,shop_id
	,CASE WHEN high_qual_listing_count > 0 THEN 1 ELSE 0 END AS inclusive_def
	,CASE WHEN high_qual_concentration = 1 THEN 1 ELSE 0 END AS strict_def
FROM seller_counts
),
prolist_seller AS (
SELECT
	shop_id
	,spend_last_4w
	,budget_30d
	,impressions_last_4w
FROM `etsy-data-warehouse-prod.rollups.prolist_daily_shop_data` p
WHERE date = current_date - 1
)
SELECT 
CASE WHEN budget_30d IS NULL THEN 'no budget'
  WHEN budget_30d IS NOT NULL AND impressions_last_4w = 0 THEN 'have a budget but no impression'
	WHEN budget_30d IS NOT NULL AND impressions_last_4w > 0 AND spend_last_4w = 0 THEN ' have a budget and impressions, but no spend'
	WHEN budget_30d IS NOT NULL AND impressions_last_4w > 0 AND spend_last_4w > 0 THEN 'have a budget, impression and spend'
	ELSE NULL END AS prolist_flag
	,inclusive_def
	,strict_def
	,COUNT(DISTINCT user_id) AS user_count
FROM qual_def s
LEFT JOIN prolist_seller USING (shop_id)
GROUP BY 1,2,3
;

