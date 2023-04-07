-- be in the wall decor category (1027), painting (105), photography (115), prints (119), drawing (75)
-- not be personalizable
-- calculate number of active variation
-- calculate number of active variation that includes dimension keywords
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.wall_deco_dimension_listing` AS (
SELECT
  l.listing_id,
  lva.listing_variation_id,
  lva.attribute_name,
  lva.attribute_value,
  l.products_count_active,
  l.state,
  COUNT(DISTINCT attribute_name) over (partition by l.listing_id) AS active_variation_count,
  COUNT(DISTINCT CASE WHEN REGEXP_CONTAINS(attribute_name,r'(?i)size|dimension|length|width') THEN attribute_name ELSE NULL END)  over (partition by l.listing_id) AS active_dimension_variation_count
FROM `etsy-data-warehouse-prod.listing_mart.listing_vw` l
JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
  ON l.taxonomy_id = t.taxonomy_id
JOIN `etsy-data-warehouse-prod.listing_mart.listing_attributes` la
  ON l.listing_id = la.listing_id
JOIN `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes` lva
  ON l.listing_id = lva.listing_id
WHERE 
-- not personalizable
  la.is_personalizable = 0 
-- the variation is active
  AND lva.is_available = 1 
-- in wall decor category OR subcategory of wall decor
 AND (split(t.full_path, '.')[safe_offset(2)]='wall_decor' OR split(t.full_path, '.')[safe_offset(1)] in ("prints", "painting", "collectibles","drawing_and_illustration"))
);

-- From those listing, find listings with a soldout option
SELECT DISTINCT(listing.listing_id)
FROM `etsy-data-warehouse-dev.nlao.wall_deco_dimension_listing` as listing
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_products` as product
ON listing.listing_id = product.listing_id
JOIN `etsy-data-warehouse-prod.etsy_shard.product_offerings` as offering
ON product.product_id = offering.product_id
JOIN `etsy-data-warehouse-prod.etsy_shard.offering_quantity` as quantity
ON offering.quantity_id = quantity.quantity_id
WHERE listing.active_variation_count = 1 -- one variation
    AND active_dimension_variation_count = 1 -- variation include dimension keywords
    AND listing.products_count_active > 0
    AND offering.state = 1
    AND quantity.quantity = 0
;

-- From those listings, find listings with a soldout option and waitlist enabled 
SELECT DISTINCT(listing.listing_id)
FROM `etsy-data-warehouse-dev.nlao.wall_deco_dimension_listing` as listing
JOIN `etsy-data-warehouse-prod.etsy_shard.listing_products` as product
ON listing.listing_id = product.listing_id
JOIN `etsy-data-warehouse-prod.etsy_shard.product_offerings` as offering
ON product.product_id = offering.product_id
JOIN `etsy-data-warehouse-prod.etsy_shard.offering_quantity` as quantity
ON offering.quantity_id = quantity.quantity_id
JOIN `etsy-data-warehouse-prod.etsy_shard.back_in_stock_waitlist_subscription` as waitlist
  ON waitlist.listing_id = listing.listing_id
WHERE listing.active_variation_count = 1 -- one variation
    AND active_dimension_variation_count = 1 -- variation include dimension keywords
    AND listing.products_count_active > 0
    AND offering.state = 1
    AND quantity.quantity = 0
    AND waitlist.is_deleted = 0 -- with waitlist
;

-- From those listings, find listings with a variation value that is 14 - 19 character long
SELECT
listing_id
FROM `etsy-data-warehouse-dev.nlao.wall_deco_dimension_listing`
WHERE active_variation_count = 1
AND active_dimension_variation_count = 1
AND variation_value_length BETWEEN 14 AND 19
;
