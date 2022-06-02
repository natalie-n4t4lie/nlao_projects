################### COVERAGE ###################

-- OVERALL
SELECT
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage,
COUNT(CASE WHEN bb.is_active = 1 THEN bb.mapped_user_id ELSE NULL END) as active_user_count,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_hobby_coverage,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.is_active = 1 AND bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage
FROM `etsy-data-warehouse-prod.user_mart.mapped_user_profile` bb
;

-- BY REGION
SELECT
bb.region_name,
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

-- PURCHASE BIN
SELECT
bb.buyer_past_year_gms_bin	,
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

SELECT
bb.buyer_lifetime_gms_bin,
COUNT(bb.mapped_user_id) as user_count,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS hobby_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS hobby_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_style_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_styles` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_style_coverage,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23' AND score >= 0.1 ) THEN mapped_user_id ELSE NULL END) AS active_animal_coverage_01_threshold,
COUNT(CASE WHEN bb.mapped_user_id IN (select user_id FROM `etsy-data-warehouse-prod.knowledge_base.buyer_favorite_animals` WHERE _date = '2022-05-23') THEN mapped_user_id ELSE NULL END) AS active_animal_coverage,
FROM `etsy-data-warehouse-prod.rollups.buyer_basics` bb
GROUP BY 1
;

-- RANK
SELECT 
display_name,
COUNT(user_id) as user_count
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` 
WHERE _date = current_date() AND score >= 0.1
GROUP BY 1
ORDER BY 2 DESC
;

-- SCORE
SELECT 
ROUND(score,2) as score,
COUNT(*) as user_count
FROM `etsy-data-warehouse-prod.knowledge_base.buyer_hobbies` 
WHERE _date = current_date()
GROUP BY 1
ORDER BY 2 DESC
;
