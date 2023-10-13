CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.nlao.listing_variation_price` AS (
SELECT
lv.listing_id,
lv.variation_name,
lv.custom_property_name,
lv.property_value,
pv.listing_variation_id,
po.product_id,
po.product_offering_id,
po.price,
po.currency_code
FROM `etsy-data-warehouse-prod.rollups.listing_variations_extended` lv -- get variation name with "quantity" keywords
JOIN `etsy-data-warehouse-prod.etsy_shard.product_variations` pv -- get variation <--> product_id
  ON lv.listing_id = pv.listing_id AND lv.listing_variation_id = pv.listing_variation_id
JOIN `etsy-data-warehouse-prod.etsy_shard.product_offerings` po -- get product_id <--> product_offering_id AND price
  ON pv.product_id = po.product_id AND pv.shop_id = po.shop_id
WHERE (LOWER(variation_name) LIKE '%quantity%' OR LOWER(custom_property_name) LIKE '%quantity%')
      AND lv.has_inventory = 1
      AND pv.state = 1
      AND po.state = 1
ORDER BY 1,2,3
)
