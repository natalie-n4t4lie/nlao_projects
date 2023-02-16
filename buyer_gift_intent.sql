--WITH GIFT INTENT PREDICTION USER COVERAGE
SELECT  
COUNT(DISTINCT b.mapped_user_id) as user_count,
COUNT(DISTINCT CASE WHEN is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_count,
COUNT(DISTINCT CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date()) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count,
COUNT(DISTINCT CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date()) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count,
COUNT(DISTINCT CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date()) THEN b.mapped_user_id ELSE NULL END) / COUNT(DISTINCT b.mapped_user_id) AS buyer_with_gift_intent_coverage,
COUNT(DISTINCT CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date()) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) / COUNT(DISTINCT CASE WHEN is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_coverage,
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
;

SELECT  
buyer_segment,
COUNT(DISTINCT CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date()) THEN b.mapped_user_id ELSE NULL END) / COUNT(DISTINCT b.mapped_user_id) AS buyer_with_gift_intent_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
GROUP BY 1
;

-- GIFTY USER COVERAGE
SELECT  
COUNT(b.mapped_user_id) as user_count,
COUNT(CASE WHEN is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_count,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count5,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.6) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count6,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.7) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count7,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.8) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count8,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.9) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count9,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count5,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.6) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count6,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.7) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count7,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.8) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count9,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.9) AND is_active = 1 THEN b.mapped_user_id ELSE NULL END) AS active_buyer_with_gift_intent_prediction_count9
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
;

SELECT  
buyer_segment,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN b.mapped_user_id ELSE NULL END) / COUNT(b.mapped_user_id) AS buyer_with_gift_intent_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
GROUP BY 1
;

SELECT  
buyer_segment,
COUNT(b.mapped_user_id) as user_count,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count5,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.6) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count6,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.7) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count7,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.8) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count8,
COUNT(CASE WHEN b.mapped_user_id IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.9) THEN b.mapped_user_id ELSE NULL END) AS buyer_with_gift_intent_prediction_count9
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
GROUP BY 1
;

--WITH GIFT INTENT PREDICTION USER GMS
SELECT  
SUM(l.gms_net_12m) as gms,
SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) AS buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) / SUM(l.gms_net_12m) AS buyer_with_gift_intent_gms_coverage,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) / SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_purch_ltd` l USING (mapped_user_id)
;


SELECT  
buyer_segment,
SUM(l.gms_net_12m) as gms,
SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) AS buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) / SUM(l.gms_net_12m) AS buyer_with_gift_intent_gms_coverage,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) / SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_purch_ltd` l USING (mapped_user_id)
GROUP BY 1
;

-- GIFTY USER GMS
SELECT  
SUM(l.gms_net_12m) as gms,
SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) AS buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) / SUM(l.gms_net_12m) AS buyer_with_gift_intent_gms_coverage,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) / SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_purch_ltd` l USING (mapped_user_id)
;


SELECT  
buyer_segment,
SUM(l.gms_net_12m) as gms,
SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) AS buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) THEN l.gms_net_12m ELSE NULL END) / SUM(l.gms_net_12m) AS buyer_with_gift_intent_gms_coverage,
SUM(CASE WHEN l.gms_net_12m IN (SELECT user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` WHERE _date = current_date() AND assignment_confidence_score> 0.5) AND is_active = 1 THEN l.gms_net_12m ELSE NULL END) / SUM(CASE WHEN is_active = 1 THEN l.gms_net_12m ELSE NULL END) AS active_buyer_with_gift_intent_gms_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
JOIN `etsy-data-warehouse-prod.user_mart.mapped_user_purch_ltd` l USING (mapped_user_id)
GROUP BY 1
;

--GIFT SCORE DISTRIBUTION
SELECT
ROUND(assignment_confidence_score,2) AS round_assignment_confidence_score,
COUNT(distinct user_id) AS user_count
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` b
JOIN `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` g
  ON b.mapped_user_id = g.user_id
WHERE _date = current_date()
GROUP BY 1
ORDER BY 1 ASC
;

--SEARCH QUERY FOR WORDCLOUD
SELECT
CASE WHEN assignment_confidence_score > 0.5 THEN 'GIFTY USER' ELSE 'NON-GIFTY USER' END AS gifty_flag,
query,
COUNT(*) AS session_count
FROM `etsy-data-warehouse-prod.weblog.recent_visits` v
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
ON v.visit_id = q.visit_id
JOIN `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` g
ON v.user_id = g.user_id
WHERE 
  v._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  AND q._date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  AND length(q.query)>2  
  AND g._date = current_date - 1
GROUP BY 1,2
ORDER BY 1,3 DESC
;

--SEARCH QUERY FOR WORDCLOUD
SELECT
CASE WHEN assignment_confidence_score > 0.5 THEN 'GIFTY USER' ELSE 'NON-GIFTY USER' END AS gifty_flag,
CASE WHEN REGEXP_CONTAINS(query,r'gift') THEN 1 ELSE 0 END AS gift_query_flag,
COUNT(*) AS session_count
FROM `etsy-data-warehouse-prod.weblog.recent_visits` v
JOIN `etsy-data-warehouse-prod.search.query_sessions_new` q
ON v.visit_id = q.visit_id
JOIN `etsy-data-warehouse-prod.knowledge_base.buyer_gift_intent` g
ON v.user_id = g.user_id
WHERE 
  v._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE()
  AND q._date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE()
  AND length(q.query)>2  
  AND g._date = current_date - 1
GROUP BY 1,2
ORDER BY 1,2
;


