

------- "bootstrap" model
-- sample
-- fit model
-- make predictions for active listings
-- average (and std dev) results across samples for each listing

-- bq --project_id etsy-bigquery-adhoc-prod query --use_legacy_sql=False < listing_quality_bq_model.sql

--- gather data
create or replace table `etsy-data-warehouse-dev.nlao.model_input_collection` as 
	with
	stash as (
		select distinct
			listing_id
		from
			`etsy-data-warehouse-prod.rollups.vetted_stash_listings`
		)
	,category_dummies as (
		select
			listing_id
			,sum(if(top_category = "accessories",1,0)) as cat_accessories
			,sum(if(top_category = "art_and_collectibles",1,0)) as cat_art_and_collectibles
			,sum(if(top_category = "bags_and_purses",1,0)) as cat_bags_and_purses
			,sum(if(top_category = "bath_and_beauty",1,0)) as cat_bath_and_beauty
			,sum(if(top_category = "books_movies_and_music",1,0)) as cat_books_movies_and_music
			,sum(if(top_category = "clothing",1,0)) as cat_clothing
			,sum(if(top_category = "craft_supplies_and_tools",1,0)) as cat_craft_supplies_and_tools
			,sum(if(top_category = "electronics_and_accessories",1,0)) as cat_electronics_and_accessories
			,sum(if(top_category = "home_and_living",1,0)) as cat_home_and_living
			,sum(if(top_category = "jewelry",1,0)) as cat_jewelry
			,sum(if(top_category = "paper_and_party_supplies",1,0)) as cat_paper_and_party_supplies
			,sum(if(top_category = "pet_supplies",1,0)) as cat_pet_supplies
			,sum(if(top_category = "shoes",1,0)) as cat_shoes
			,sum(if(top_category = "toys_and_games",1,0)) as cat_toys_and_games
			,sum(if(top_category = "weddings",1,0)) as cat_weddings
		from
			`etsy-data-warehouse-prod.listing_mart.listing_attributes`
		group by 1
		)
	,seller_tier_dummies as (
		select
			shop_id
			-- ,sum(if(seller_tier = "active seller",1,0)) as active_seller -- 0 weight
			,sum(if(seller_tier = "high potential seller",1,0)) as st_high_potential_seller
			-- ,sum(if(seller_tier = "non active seller",1,0)) as st_non_active_seller
			,sum(if(seller_tier = "power seller",1,0)) as st_power_seller
			,sum(if(seller_tier = "seller with a sale",1,0)) as st_seller_with_a_sale
			,sum(if(seller_tier = "top seller",1,0)) as st_top_seller
		from
			`etsy-data-warehouse-prod.rollups.seller_basics`
		group by 1
		)
	,mfy_dummies as (
		select
			listing_id
			,sum(if(custom_level = "1 - no_MFY_no_variation",1,0)) as mfy_no_MFY_no_variation
			,sum(if(custom_level = "2 - no_MFY_has_variation",1,0)) as mfy_no_MFY_has_variation
			,sum(if(custom_level = "3 - light",1,0)) as mfy_light
			,sum(if(custom_level = "4 - heavy",1,0)) as mfy_heavy
		from
			`etsy-data-warehouse-prod.rollups.active_listing_shipping_costs`
		group by 1
		)
	select
		l.listing_id
		,l.price_usd
		,coalesce(g.past_year_gms,0) as past_year_gms
		,coalesce(g.past_year_orders,0) as past_year_orders
		,coalesce(v.variation_count,0) as variations
		,case when sb.country_id = 209 then 1 else 0 end as us_seller
		,cd.* except (listing_id)
		,std.* except (shop_id)
		,md.* except (listing_id)
		,lm.is_vintage
		-- aspects of fulfillment/seller quality
		-- listing views?
		,case when s.listing_id is null then 0 else 1 end as is_vetted
	from
		`etsy-data-warehouse-prod.listing_mart.listings` l
		inner join category_dummies cd
			on l.listing_id = cd.listing_id
		inner join mfy_dummies md
			on l.listing_id = md.listing_id
		left join stash s
			on l.listing_id = s.listing_id
		left join `etsy-data-warehouse-prod.listing_mart.listing_gms` g
			on l.listing_id = g.listing_id
		left join `etsy-data-warehouse-prod.listing_mart.listing_variations` v
			on l.listing_id = v.listing_id
		left join `etsy-data-warehouse-prod.rollups.seller_basics` sb
			on l.shop_id = sb.shop_id
		left join seller_tier_dummies std
			on l.shop_id = std.shop_id
		left join `etsy-data-warehouse-prod.materialized.listing_marketplaces` as lm
			on l.listing_id = lm.listing_id
	where
		l.is_active = 1
		and sb.gms_vigintile is not null
;

--- create a table to save model diagnostics
create or replace table `etsy-data-warehouse-dev.nlao.qual_model_diagnostics`
(
	run INT64
	,precision FLOAT64
	,recall  FLOAT64
	,accuracy FLOAT64
	,f1_score FLOAT64
	,log_loss FLOAT64
	,roc_auc FLOAT64
)
;

--- create a table to save weights
create or replace table `etsy-data-warehouse-dev.nlao.qual_model_weights`
(
	run INT64
	,feature STRING
	,attribution FLOAT64
)
;

---
create or replace table `etsy-data-warehouse-dev.nlao.quality_model_output_runs`
(
	run INT64
	,listing_id INT64
	,prob_is_vetted FLOAT64
)
;


begin
declare number_of_resamples int64;

set number_of_resamples = 0;

while number_of_resamples < 5 do

	------- generate balanced input sample
	create or replace table `etsy-data-warehouse-dev.nlao.model_input` as 
		with
		sampled_rando as (
			select
				*
				,RAND() as rando
			from
				`etsy-data-warehouse-dev.nlao.model_input_collection`
			where
				is_vetted = 0
		)
		,sampled as (
			select
				* except(rando)
				,(select count(*) from `etsy-data-warehouse-dev.nlao.model_input_collection` where is_vetted = 1) as balancing
				,row_number() over (order by rando) as rn
			from
				sampled_rando
		)
		,holder as (
			select * except(balancing,rn) from sampled where rn <= balancing
			union all
			select * from `etsy-data-warehouse-dev.nlao.model_input_collection` where is_vetted = 1
		)
		select
			*
		  ,CASE
		    WHEN MOD(listing_id, 10) < 8 THEN "training"
		    else "evaluation"
		    -- WHEN MOD(listing_id, 10) = 8 THEN "evaluation"
		    -- WHEN MOD(listing_id, 10) = 9 THEN "prediction"
		  END AS dataframe
		from
			holder
	;

	------- create model
	CREATE OR REPLACE MODEL
	  `etsy-data-warehouse-dev.nlao.quality_model`
	OPTIONS
	  ( model_type="LOGISTIC_REG",
	    auto_class_weights=TRUE,
	    enable_global_explain=TRUE,
	    input_label_cols=["is_vetted"]
	  ) AS
	SELECT
	  * EXCEPT(listing_id, dataframe)
	FROM
	  `etsy-data-warehouse-dev.nlao.model_input`
	WHERE
	  dataframe = "training"
	;

	---- model diagnostics
	insert into `etsy-data-warehouse-dev.nlao.qual_model_diagnostics`
		SELECT
			-- 0 + 1 as run
			number_of_resamples + 1 as run
		  ,*
		FROM
		  ML.EVALUATE (MODEL `etsy-data-warehouse-dev.nlao.quality_model`,
		    (
				SELECT
				  *
				FROM
				  `etsy-data-warehouse-dev.nlao.model_input`
				WHERE
				  dataframe = "evaluation"
		    )
		  )
	 ;

		--- weighted influence
	insert into `etsy-data-warehouse-dev.nlao.qual_model_weights`
		select
			0 + 1 as run
			-- number_of_resamples + 1 as run
			,*
			-- ,sum(attribution) over ()
		from
			ML.GLOBAL_EXPLAIN (model `etsy-data-warehouse-dev.nlao.quality_model`)
		;

	---- prediction
	insert into `etsy-data-warehouse-dev.nlao.quality_model_output_runs`
		SELECT
			-- 0 + 1 as run
			number_of_resamples + 1 as run
		  ,listing_id
		  ,(select prob from unnest(predicted_is_vetted_probs) where label = 1) as prob_is_vetted
		FROM
		  ML.PREDICT (MODEL `etsy-data-warehouse-dev.nlao.quality_model`,
		    (
		    SELECT
		      *
		    FROM
		      `etsy-data-warehouse-dev.nlao.model_input_collection`
		     )
		  )
		-- limit 10
		;

	set number_of_resamples = number_of_resamples + 1;
	end while;

end;


-- select count(*), count(distinct listing_id),count(*) - count(distinct listing_id) from `etsy-data-warehouse-dev`.pgoldberg.quality_model_output_runs;


create or replace table `etsy-data-warehouse-dev.nlao.quality_model_output` as 
	with
	aggs as (
		select
			listing_id
			,avg(prob_is_vetted) as quality_score
			,min(prob_is_vetted) as quality_score_min
			,max(prob_is_vetted) as quality_score_max
		from
			`etsy-data-warehouse-dev.nlao.quality_model_output_runs`
		group by 1
	)
	,devs as (
		select distinct
			listing_id
			,stddev(prob_is_vetted) over (partition by listing_id) as std_dev
		from
			`etsy-data-warehouse-dev.nlao.quality_model_output_runs`
	)
	select
		a.*
		,std_dev
	from
		aggs a
		inner join devs d
			on a.listing_id = d.listing_id
;

