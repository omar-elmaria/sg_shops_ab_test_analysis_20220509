-- This is the complete data extraction script for AB tests with target groups launched through the DPS UI
-- V2 includes the country code filter

-- Step 1: Declare the input variables used throughout the script
DECLARE test_name_var ARRAY <STRING>;
DECLARE exp_id_sg_shops INT64;
DECLARE entity ARRAY <STRING>;
DECLARE country_code_var ARRAY <STRING>;
DECLARE od_status STRING;
DECLARE v_type ARRAY <STRING>;
DECLARE parent_vertical ARRAY <STRING>;
DECLARE variants ARRAY <STRING>;
DECLARE exp_target_groups ARRAY <STRING>;
DECLARE start_date, end_date DATE;
DECLARE target_group_variant_scheme_id_valid_combos_sg_shops ARRAY <STRING>;
SET test_name_var = ['SG_20220509_L_B_O_NV'];
SET exp_id_sg_shops = 23;
SET entity = ['FP_SG'];
SET country_code_var = ['sg'];
SET od_status = 'OWN_DELIVERY'; -- Name of the test in the AB test dashboard/DPS experiments tab
SET v_type = ['health_and_wellness', 'groceries'];
SET parent_vertical = ['shop', 'Shop']; -- Different parent vertical names depending on the platform. APAC is 'Restaurant' (the two other variations are 'restaurant' and 'restaurants'). Shops are --> [shop, Shop]. Apac is 'Shop'
SET variants = ['Variation1', 'Variation2', 'Control'];
SET exp_target_groups = ['TG1', 'TG2', 'TG3', 'TG4'];
SET (start_date, end_date) = (DATE('2022-05-09'), DATE('2022-06-13')); -- Encompasses the entire duration of the hybrid test 
SET target_group_variant_scheme_id_valid_combos_sg_shops = ['TG1 | Control | 2616', 'TG1 | Variation1 | 2616', 'TG1 | Variation2 | 3156',
                                                            'TG2 | Control | 2610', 'TG2 | Variation1 | 3157', 'TG2 | Variation2 | 2610',
                                                            'TG3 | Control | 1674', 'TG3 | Variation1 | 1674', 'TG3 | Variation2 | 3158',
                                                            'TG4 | Control | 1673', 'TG4 | Variation1 | 3159', 'TG4 | Variation2 | 1673'];

/*
Notes to self:
- There are so many **Non_TG sessions in the cvr_data table** (contrary to the orders table) due to overlapping zones. Some Non_TG vendors in the dps_cvr_events table are associated with multiple zones (some of them could be our targeted ones).
However, in the orders table, they are tagged with completely different zones than the targeted ones because an order can only have one zone (that's BI's logic). This causes these orders to NOT be caught and the number of Non_TG **orders** to be so little, contrary
to the number of Non_TG **sessions**

- You can fix the **Non_TG sessions issue** by filtering out the sessions with Non_TG (i.e., not analysing them at all) OR filtering for the targeted zones by the **ST_CONTAINS** instead of **zone_id**. This applies to the sessions_mapped_to_orders_v2 table
as well as dps_cvr_events. However, these two tables do NOT have the location field. You will need to get it from cDWH.orders AND dps_sessions_mapped_to_ga_sessions
*/

----------------------------------------------------------------END OF THE INPUT SECTION----------------------------------------------------------------

-- Step 2: Extract the vendor IDs per target group
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_loved_brands_sg_shops` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    CONCAT('TG', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
WHERE TRUE
    AND entity_id IN UNNEST(entity)
    AND country_code IN UNNEST(country_code_var)
    AND test_name IN UNNEST(test_name_var)
ORDER BY 1,2,3,4,5;

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_zone_ids_loved_brands_sg_shops` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_name,
    test_id,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
WHERE TRUE
    AND entity_id IN UNNEST(entity)
    AND country_code IN UNNEST(country_code_var)
    AND test_name IN UNNEST(test_name_var) 
ORDER BY 1,2;

-- Step 4: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_loved_brands_sg_shops` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
    tgt.test_name,
    tgt.test_id
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_loved_brands_sg_shops` tgt ON p.entity_id = tgt.entity_id AND co.country_code = tgt.country_code AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

-- Step 5: Pull the business and logisitcal KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_loved_brands_sg_shops` AS
WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.costs) AS delivery_costs_local,
        SUM(p.costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
),

entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.tg_name, 'Non_TG') AS target_group,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    a.vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (pick the ones that are applicable to your test)
    a.delivery_fee_local, -- The delivery fee amount of the dps session.
    a.dps_travel_time_fee_local, -- The (dps_delivery_fee - dps_surge_fee) of the dps session.
    a.dps_minimum_order_value_local AS mov_local, -- The minimum order value of the dps session.
    a.dps_surge_fee_local, -- The surge fee amount of the session.
    a.dps_delivery_fee_local,
    a.service_fee AS service_fee_local, -- The service fee amount of the session.
    a.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    a.gfv_local, -- The gfv (gross food value) of the order placed from backend
    a.standard_fee, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
    a.commission_local,
    a.commission_base_local,
    a.joker_vendor_fee_local,
    ROUND(a.commission_local / NULLIF(a.commission_base_local, 0), 4) AS commission_rate,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    cst.delivery_costs_local,
    
    -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) AS revenue_eur,

    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) - cst.delivery_costs_local / a.exchange_rate AS gross_profit_eur,

    -- Logistics KPIs
    a.dps_mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.dps_mean_delay_zone_id, 
    a.dps_travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT,

    -- Centra DWH fields	
    dwh.value.delivery_fee_local AS delivery_fee_local_cdwh,	
    dwh.value.delivery_fee_vat_local AS delivery_fee_vat_local_cdwh,
    dwh.value.voucher_dh_local AS voucher_dh_local_cdwh,	
    dwh.value.voucher_other_local AS voucher_other_local_cdwh,	
    dwh.value.discount_dh_local AS discount_dh_local_cdwh,	
    dwh.value.discount_other_local AS discount_other_local_cdwh,	
    dwh.value.joker_customer_discount_local AS joker_customer_discount_local_cdwh,
    dwh.value.joker_vendor_fee_local AS joker_vendor_fee_local_cdwh,
    dwh.is_joker,
    dwh.value.gbv_local AS gfv_local_cdwh,
    dwh.value.customer_paid_local AS customer_paid_local_cdwh,
    dwh.value.mov_local AS mov_local_cdwh,
    dwh.value.mov_customer_fee_local AS sof_cdwh,
    dwh.payment_method,
    dwh.payment_type,

    -- Pandata fields
    pd.service_fee_total_local AS service_fee_total_local_pd,
    pd.container_price_local AS container_price_local_pd,
    pd.delivery_fee_local AS delivery_fee_local_pd,
    pd.delivery_fee_forced_local AS delivery_fee_forced_local_pd,
    pd.delivery_fee_original_local AS delivery_fee_original_local_pd,
    pd.delivery_fee_vat_rate AS delivery_fee_vat_rate_pd,	
    pd.product_vat_groups AS product_vat_groups_pd,	
    pd.vat_rate AS vat_rate_pd,
    pd.delivery_fee_vat_local AS delivery_fee_vat_local_pd,	
    pd.products_vat_amount_local AS products_vat_amount_local_pd,
    pd.vat_amount_local AS vat_amount_local_pd,

    -- Special fields
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
        )
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END) -- If the order comes from a non-Pandora country, use delivery_fee_local
    END AS actual_df_paid_by_customer,
    a.is_delivery_fee_covered_by_discount,
    a.is_delivery_fee_covered_by_voucher,
    CASE WHEN is_delivery_fee_covered_by_discount = FALSE AND is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher' ELSE 'DF Voucher' END AS df_voucher_flag,
    CASE WHEN pdos.is_free_delivery_subscription_order = TRUE THEN 'Subscription FD Order' ELSE 'Non-Subscription FD Order' END AS fd_subscription_flag, -- Only two possible values --> True or False
    pd.minimum_delivery_value_local AS mov_pd
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_sb_subscriptions` pdos ON pd.uuid = pdos.uuid AND pd.created_date_utc = pdos.created_date_utc
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
-- IMPORTANT NOTE: CHECK WHETHER YOU NEED INNER JOIN OR LEFT JOIN (In the case of LBs, we want INNER JOIN for the zones, but LEFT JOIN for the vendors, as we compare variants for LBs AND all vendors)
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
-- IMPORTANT NOTE: To filter for orders coming from the target zones, you can either INNER JOIN on `dh-logistics-product-ops.pricing.ab_test_zone_ids_loved_brands_sg_shops` **OR** LEFT JOIN on  
-- INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_loved_brands_sg_shops` zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id -- Filter for orders from vendors in the target zones `dh-logistics-product-ops.pricing.ab_test_geo_data_loved_brands_sg_shops` AND add a delivery location condition to the WHERE clause
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_loved_brands_sg_shops` zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_loved_brands_sg_shops` tg ON a.entity_id = tg.entity_id AND a.vendor_id = tg.vendor_code AND a.experiment_id = tg.test_id -- Tag the vendors with their target group association
WHERE TRUE
    AND a.entity_id IN UNNEST(entity)
    AND a.country_code IN UNNEST(country_code_var)
    AND a.created_date BETWEEN start_date AND end_date
    AND a.variant IN UNNEST(variants)
    -- AND a.is_own_delivery -- OD or MP (Comment out to include MP vendors in the non-TG)
    AND a.vertical_type IN UNNEST(v_type) -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
    AND a.delivery_status = 'completed' -- Successful orders
    AND experiment_id IN (exp_id_sg_shops) -- Filter for the right experiment
    AND vendor_vertical_parent IN UNNEST(parent_vertical) -- Necessary filter in the case of parallel experiments
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

----------------------------------------------------------------END OF RAW ORDERS PART----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_loved_brands_sg_shops` AS
SELECT -- All Target groups (separately)
    *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_loved_brands_sg_shops`
WHERE TRUE
    AND CONCAT(target_group, ' | ', variant, ' | ', scheme_id) IN UNNEST (target_group_variant_scheme_id_valid_combos_sg_shops) -- Filter for the orders from TG1 that you want to include in your analysis

UNION ALL

SELECT -- Non_TG 
    *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_loved_brands_sg_shops`
WHERE TRUE
    AND target_group = 'Non_TG';

----------------------------------------------------------------END OF BUSINESS KPIs PART----------------------------------------------------------------

-- Step 6: Pull pricing data from the DPS logs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_logs_data_loved_brands_sg_shops` AS
WITH dps_logs_stg_1 AS ( -- Will be used to get the the MOV and DF values of the session
    SELECT DISTINCT
        logs.entity_id,
        logs.created_date,
        endpoint,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        ex.id AS experiment_id,
        ex.variant,
        v.id AS vendor_code,
        v.meta_data.vendor_price_scheme_type,
        v.meta_data.scheme_id,
        v.vertical_parent,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` logs
LEFT JOIN UNNEST(vendors) v
LEFT JOIN UNNEST(customer.experiments) ex
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_loved_brands_sg_shops` cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) AND ex.id = cd.test_id -- Filter for sessions in the zones of the experiment
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` vv2 ON vv2.entity_id = logs.entity_id AND vv2.vendor_code = v.id -- Used to get the vertical
WHERE TRUE -- No need for an endpoint filter here
    AND logs.entity_id IN UNNEST(entity)
    AND LOWER(logs.country_code) IN UNNEST(country_code_var)
    AND logs.created_date BETWEEN start_date AND end_date
    AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
    AND ex.variant IN UNNEST(variants) -- Filter for the variants that are part of the test.
    AND ex.id IN (exp_id_sg_shops) -- Filter for the right experiment
    AND vv2.vertical_type IN UNNEST(v_type) -- Filter for the right verticals in the experiment
    AND v.vertical_parent IN UNNEST(parent_vertical) -- Necessary filter in the case of parallel experiments
),

dps_logs_stg_2 AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entity_id, experiment_id, dps_session_id, vendor_code ORDER BY created_at DESC) AS row_num_dps_logs -- Create a row counter to take the last delivery fee/MOV seen in the session. We assume that this is the one that the customer took their decision to purchase/not purchase on
    FROM dps_logs_stg_1
),

dps_logs AS (
    SELECT *
    FROM dps_logs_stg_2 
    WHERE row_num_dps_logs = 1 -- Take the last DF/MOV seen by the customer during the session
)
SELECT * FROM dps_logs;

----------------------------------------------------------------SEPARATOR----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_loved_brands_sg_shops` AS
SELECT DISTINCT
    x.created_date, -- Date of the ga session
    x.entity_id, -- Entity ID
    x.platform, -- Operating system (iOS, Android, Web, etc.)
    x.brand, -- Talabat, foodpanda, Foodora, etc.
    x.events_ga_session_id, -- GA session ID
    x.fullvisitor_id, -- The visit_id defined by Google Analytics
    x.visit_id, -- 	The visit_id defined by Google Analytics
    x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
    x.total_transactions, -- The total number of transactions in the GA session
    x.ga_dps_session_id, -- DPS session ID

    x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
    x.sessions.endpoint, -- The endpoint from where the DPS request is coming, including MultipleFee, which could come from Listing Page or others. and SingleFee, which could come from Menu page or others
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
    x.sessions.experiment_id, -- Experiment ID in the DPS logs
    x.sessions.vertical_parent, -- Parent vertical in the DPS logs
    x.sessions.customer_status, -- The customer.tag in the DPS logs, indicating whether the customer is new or not
    x.sessions.location, -- The customer.location in the DPS logs
    x.sessions.variant_concat, -- The concatenation of all the existing variants in the DPS logs for the dps session id. There might be multiple variants due to location changes or session timeout
    x.sessions.location_concat, -- The concatenation of all the existing locations in the DPS logs for the dps session id
    x.sessions.customer_status_concat, -- 	The concatenation of all the existing customer.tag in the DPS logs for the dps session id

    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    -- Records where the event_type = home_screen.loaded OR shop_list.loaded will have vendor_code = NULL, so this field will take the value of 'Non_TG' for such records
    CASE WHEN e.vendor_code IS NULL THEN 'Unknown' ELSE COALESCE(tg.tg_name, 'Non_TG') END AS target_group,
    e.vertical_type,
    e.event_time, -- The timestamp of the event's creation.
    e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
    e.expedition_type, -- The delivery type of the session, pickup or delivery

    dps.city_id, -- City ID based on the DPS session
    dps.city_name, -- City name based on the DPS session
    dps.id AS zone_id, -- Zone ID based on the DPS session
    dps.name AS zone_name, -- Zone name based on the DPS session
    dps.timezone, -- Time zone of the city based on the DPS session

    ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` x
LEFT JOIN UNNEST(events) e
LEFT JOIN UNNEST(dps_zone) dps
-- IMPORTANT NOTE: WE INNER JOIN ON THE TARGET ZONES, THEN LEFT JOIN ON THE TARGET GROUPS TABLE
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_loved_brands_sg_shops` zn ON zn.entity_id = x.entity_id AND dps.id = zn.zone_id  -- Filter for sessions in the target zones
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_loved_brands_sg_shops` tg ON tg.entity_id = x.entity_id AND tg.vendor_code = e.vendor_code -- Tag the vendors with their target group association
WHERE TRUE
/* 
No need to check whether the vendors are OD or MP. Most of the time, MP vendors will be non-TG by default. If you want to know the delivery_type of the vendor, use this join 
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` vv2 ON vv2.entity_id = e.entity_id AND vv2.vendor_code = ven_id -- Used to get the vertical
CROSS JOIN UNNEST(delivery_provider) AS delivery_type 
Beware that it produces duplicates because a vendor ID can be linked to "OWN_DELIVERY" and "PICKUP" at the same time
*/
    AND x.entity_id IN UNNEST(entity)
    AND x.country_code IN UNNEST(country_code_var)
    AND x.created_date BETWEEN start_date AND end_date
    AND x.sessions.variant IN UNNEST(variants)
    AND x.sessions.experiment_id IN (exp_id_sg_shops) -- Filter for the right experiment
    AND (e.vertical_type IN UNNEST(v_type) OR e.vertical_type IS NULL) -- We include "vertical_type IS NULL" so that we do NOT filter out these event types --> 'home_page.loaded' and 'shop_list.loaded'
    AND x.sessions.vertical_parent IN UNNEST(parent_vertical); -- Necessary filter in the case of parallel experiments

----------------------------------------------------------------SEPARATOR----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_loved_brands_sg_shops` AS
SELECT
    x.*,
    logs.vendor_price_scheme_type,
    logs.scheme_id,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_sessions_mapped_to_ga_sessions_data_loved_brands_sg_shops` x
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_dps_logs_data_loved_brands_sg_shops` logs -- LEFT JOIN preserves the records in the DPS logs for MP vendors (which usually belong to the Non-TG)
    ON TRUE
    AND x.entity_id = logs.entity_id 
    AND x.ga_dps_session_id = logs.dps_session_id 
    AND x.created_date = logs.created_date 
    AND x.vendor_code = logs.vendor_code -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
ORDER BY x.entity_id, x.experiment_id, x.created_date, x.events_ga_session_id, x.perseus_client_id, x.vendor_code;

-- Step 6: Drop incorrect records
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` AS
SELECT 
    *
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_loved_brands_sg_shops`
WHERE TRUE 
    AND (
        CASE
            WHEN experiment_id = exp_id_sg_shops AND (
                target_group = 'Non_TG' OR 
                target_group = 'Unknown' OR 
                CONCAT(target_group, ' | ', variant, ' | ', scheme_id) IN UNNEST(target_group_variant_scheme_id_valid_combos_sg_shops)
            ) THEN 'Keep'
            
        ELSE 'Drop' END
    ) = 'Keep';

----------------------------------------------------------------SUPPLEMENTARY PART----------------------------------------------------------------

-- Step 7.1: Amalgamating all events of a GA session into a string
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_all_events_per_ga_session_loved_brands_sg_shops` AS
SELECT
    created_date,
    entity_id,
    experiment_id,
    target_group,
    variant,
    events_ga_session_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT event_action IGNORE NULLS ORDER BY event_action), ", ") AS all_events_per_ga_session,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
GROUP BY 1,2,3,4,5,6;

-- Step 7.2: Amalgamating all the target groups seen in a GA session into a string
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_treatment_scope_flag_loved_brands_sg_shops` AS
SELECT
    created_date,
    entity_id,
    experiment_id,
    variant,
    events_ga_session_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT target_group IGNORE NULLS ORDER BY target_group), ", ") AS all_tgs_per_ga_session,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT vendor_code IGNORE NULLS ORDER BY vendor_code), ", ") AS all_vendor_codes_per_ga_session,
    -- If the session contains at least one vendor in a target group, it is considered a session in treatment
    CASE WHEN REGEXP_CONTAINS(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT target_group IGNORE NULLS ORDER BY target_group), ", "), r'TG[0-9]') THEN 'Y' ELSE 'N' END AS session_in_treatment_flag
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE target_group != 'Unknown' -- Eliminate records with event types 'home_page.loaded' and 'shop_list.loaded' where vendor_code is not recorded (i.e., target_group = 'Unknown')
GROUP BY 1,2,3,4,5;

-- Step 7.3: Join the previous two tables on `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops`
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` AS
SELECT 
    a.*,
    c.all_events_per_ga_session,
    b.all_tgs_per_ga_session,
    b.all_vendor_codes_per_ga_session,
    b.session_in_treatment_flag
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` a
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_treatment_scope_flag_loved_brands_sg_shops` b
    ON TRUE 
        AND a.created_date = b.created_date 
        AND a.entity_id = b.entity_id 
        AND a.experiment_id = b.experiment_id
        AND a.variant = b.variant
        AND a.events_ga_session_id = b.events_ga_session_id
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_all_events_per_ga_session_loved_brands_sg_shops` c 
    ON TRUE 
        AND a.created_date = c.created_date 
        AND a.entity_id = c.entity_id 
        AND a.experiment_id = c.experiment_id
        AND a.target_group = c.target_group
        AND a.variant = c.variant
        AND a.events_ga_session_id = c.events_ga_session_id;

----------------------------------------------------------------END OF RAW CVR TABLE ENRICHMENT PART----------------------------------------------------------------

-- Step 7.3: Aggregating CVR, sessions, and user data. We can only calculate CVR3, mCVR3, and mCVR4 because CVR and CVR2 do not have vendors/target groups associated with them. We can only compute CVR2 and CVR on the treatment or experiment levels 
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr3_data_cleaned_loved_brands_sg_shops_overall` AS
SELECT -- All target groups + non-TG (separately)
    e.entity_id,
    e.experiment_id,
    target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4

UNION ALL

SELECT -- All target groups + non-TG combined (i.e., experiment level)
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag IN ('Y', 'N') -- Equivalent to target_group IN ('TGx', 'Non_TG')  
    AND e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4

UNION ALL

SELECT -- All target groups combined (i.e., treatment scope)
    e.entity_id,
    e.experiment_id,
    'TS' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag = 'Y' -- Equivalent to target_group = TGx 
    AND e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

----------------------------------------------------------------END OF CVR3 PER TG_OVERALL PART----------------------------------------------------------------

-- Step 8: Pull CVR data from ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops (PER DAY)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr3_data_cleaned_loved_brands_sg_shops_per_day` AS
SELECT -- All target groups + non-TG (separately)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- All target groups + non-TG combined (i.e., experiment level)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag IN ('Y', 'N') -- Equivalent to target_group IN ('TGx', 'Non_TG')  
    AND e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- All target groups combined (i.e., treatment scope)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TS' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_menu_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='checkout.loaded' THEN e.events_ga_session_id ELSE NULL END) AS checkout_sessions,
    COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) AS checkout_transaction_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_details.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR3,
    ROUND(
        COUNT(DISTINCT CASE WHEN all_events_per_ga_session LIKE '%checkout%' AND all_events_per_ga_session LIKE '%transaction%' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action = 'checkout.loaded' THEN e.events_ga_session_id ELSE NULL END), 0) 
    , 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag = 'Y' -- Equivalent to target_group = TGx 
    AND e.target_group != 'Unknown' -- Eliminate records where target_group = Unknown (i.e., vendor_vode = NULL). This comes as a result of the event types --> home_page.loaded and shop_list.loaded. We do this so that we don't a have a proliferation of Non_TG records
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

----------------------------------------------------------------END OF CVR3 PER TG_PER DAY PART----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr2_data_cleaned_loved_brands_sg_shops_overall` AS
SELECT -- CVR and CVR2 (Treatment Scope)
    e.entity_id,
    e.experiment_id,
    'TS' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_list_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT e.events_ga_session_id), 0)
    , 4) AS CVR,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR2,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag = 'Y' -- Equivalent to target_group = 'TGx'
GROUP BY 1,2,3,4

UNION ALL

SELECT -- CVR and CVR2 (Non-Treatment Scope or Non-TG)
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_list_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT e.events_ga_session_id), 0)
    , 4) AS CVR,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR2,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE session_in_treatment_flag IN ('Y', 'N') -- NOT Equivalent to target_group = 'Non_TG' because a session where target_group = 'Non_TG' could also have other target groups linked to it. session_in_treatment_flag = 'N' means that the only vendors seen in the sessions are non_TG vendors
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

----------------------------------------------------------------END OF CVR2 PER TG_OVERALL PART----------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr2_data_cleaned_loved_brands_sg_shops_per_day` AS
SELECT -- CVR and CVR2 (Treatment Scope)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TS' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_list_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT e.events_ga_session_id), 0)
    , 4) AS CVR,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR2,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE TRUE 
    AND session_in_treatment_flag = 'Y' -- Equivalent to target_group = 'TGx'
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- CVR and CVR2 (Non-Treatment Scope or Non-TG)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.events_ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END) AS shop_list_sessions,
    COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) AS transactions,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT vendor_code) AS vendor_count,
    COUNT(DISTINCT zone_id) AS zone_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT e.events_ga_session_id), 0)
    , 4) AS CVR,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_action ='transaction' THEN e.events_ga_session_id ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_action ='shop_list.loaded' THEN e.events_ga_session_id ELSE NULL END), 0)
    , 4) AS CVR2,
FROM `dh-logistics-product-ops.pricing.ab_test_dps_logs_and_ga_sessions_combined_cleaned_loved_brands_sg_shops` e
WHERE session_in_treatment_flag IN ('Y', 'N') -- NOT Equivalent to target_group = 'Non_TG' because a session where target_group = 'Non_TG' could also have other target groups linked to it. session_in_treatment_flag = 'N' means that the only vendors seen in the sessions are non_TG vendors
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

----------------------------------------------------------------END OF CVR2 PER TG_OVERALL PART----------------------------------------------------------------

-- Combine CVR2 and CVR3 together (overall)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops_overall` AS
SELECT 
    a.*,
    b.shop_list_sessions,
    b.CVR,
    b.CVR2
FROM `dh-logistics-product-ops.pricing.ab_test_cvr3_data_cleaned_loved_brands_sg_shops_overall` a
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_cvr2_data_cleaned_loved_brands_sg_shops_overall` b USING (entity_id, experiment_id, target_group, variant);

-- Combine CVR2 and CVR3 together (per day)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops_per_day` AS
SELECT 
    a.*,
    b.shop_list_sessions,
    b.CVR,
    b.CVR2
FROM `dh-logistics-product-ops.pricing.ab_test_cvr3_data_cleaned_loved_brands_sg_shops_per_day` a
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_cvr2_data_cleaned_loved_brands_sg_shops_per_day` b USING (created_date, entity_id, experiment_id, target_group, variant)