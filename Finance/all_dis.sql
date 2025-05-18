CREATE OR REPLACE PROCEDURE all_dis_proc() 
AS $$ 
	BEGIN 
	DROP table IF EXISTS analytics.all_dis;
	CREATE TABLE analytics.all_dis AS 

 (



with upfront as (
SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city, charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN upfront END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN upfront END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN upfront END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN upfront END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN upfront END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN upfront END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN upfront END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN upfront END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN upfront END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN upfront END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN upfront END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN upfront END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN upfront END) AS april_2025
FROM 
    analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8
)

,coupon_code as (
    SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur,  city, charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN coupon_code END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN coupon_code END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN coupon_code END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN coupon_code END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN coupon_code END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN coupon_code END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN coupon_code END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN coupon_code END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN coupon_code END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN coupon_code END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN coupon_code END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN coupon_code END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN coupon_code END) AS april_2025
FROM 
    analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8
)

, referral_promo as (
SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur,  city,  charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN referral_promo END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN referral_promo END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN referral_promo END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN referral_promo END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN referral_promo END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN referral_promo END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN referral_promo END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN referral_promo END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN referral_promo END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN referral_promo END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN referral_promo END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN referral_promo END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN referral_promo END) AS april_2025
FROM 
	analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8
)

, referral_code as (
SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur,  city, charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN REFERRAL_CODE END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN REFERRAL_CODE END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN REFERRAL_CODE END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN REFERRAL_CODE END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN REFERRAL_CODE END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN REFERRAL_CODE END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN REFERRAL_CODE END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN REFERRAL_CODE END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN REFERRAL_CODE END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN REFERRAL_CODE END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN REFERRAL_CODE END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN REFERRAL_CODE END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN REFERRAL_CODE END) AS april_2025
FROM
    analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8

)

, payment as (
SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur,  city, charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN payment END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN payment END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN payment END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN payment END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN payment END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN payment END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN payment END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN payment END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN payment END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN payment END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN payment END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN payment END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN payment END) AS april_2025
FROM 
    analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8
)

, loyalty as (
SELECT distinct
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city, charged_till_date,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN loyalty END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN loyalty END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN loyalty END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN loyalty END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN loyalty END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN loyalty END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN loyalty END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN loyalty END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN loyalty END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN loyalty END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN loyalty END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN loyalty END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN loyalty END) AS april_2025
FROM 
    analytics.revenue
GROUP BY 1,2,3,4,5,6,7,8
)

, total as (

SELECT * FROM upfront

UNION ALL 

SELECT * FROM coupon_code

UNION ALL 

SELECT * FROM referral_code

UNION ALL 

SELECT * FROM referral_promo

UNION ALL 

SELECT * FROM loyalty

UNION ALL 

SELECT * FROM payment
)

SELECT 
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city, charged_till_date,
    SUM(april_2024) AS april_2024,
    SUM(may_2024) AS may_2024,
    SUM(june_2024) AS june_2024,
    SUM(july_2024) AS july_2024,
    SUM(august_2024) AS august_2024,
    SUM(september_2024) AS september_2024,
    SUM(october_2024) AS october_2024,
    SUM(november_2024) AS november_2024,
    SUM(december_2024) AS december_2024,
    SUM(january_2025) AS january_2025,
    SUM(february_2025) AS february_2025,
    SUM(march_2025) AS march_2025,
    SUM(april_2025) AS april_2025
FROM total
-- WHERE accountable_entity_id in ('588336','744688','749295')
--WHERE vertical = 'FURLENCO_RENTAL'
GROUP BY 1,2,3,4,5,6,7,8




) ;
END ;
$$ LANGUAGE plpgsql; 

 CALL all_dis_proc()
