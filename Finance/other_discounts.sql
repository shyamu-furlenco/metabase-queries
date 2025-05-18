Create materialized view other_discounts as 

with other_discounts as (
SELECT 
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city,
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
    revenue
[[AND fur_id = {{fur_id}}]] [[AND accountable_entity_id = {{accountable_entity_id}}]] [[AND accountable_entity_type = {{accountable_entity_type}}]]
[[AND lower(external_reference_type) = lower({{reference_type}})]]
GROUP BY 1,2,3,4,5,6,7

UNION ALL 

SELECT 
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city, 
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
    modified_revenue
WHERE 1=1 
[[AND fur_id = {{fur_id}}]] [[AND accountable_entity_id = {{accountable_entity_id}}]] [[AND accountable_entity_type = {{accountable_entity_type}}]]
[[AND lower(external_reference_type) = lower({{reference_type}})]]
GROUP BY 1,2,3,4,5,6,7

UNION ALL 

SELECT 
    fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur,city,
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
    modified_revenue
WHERE 1=1 
[[AND fur_id = {{fur_id}}]] [[AND accountable_entity_id = {{accountable_entity_id}}]] [[AND accountable_entity_type = {{accountable_entity_type}}]]
[[AND lower(external_reference_type) = lower({{reference_type}})]]
GROUP BY 1,2,3,4,5,6,7
)
SELECT fur_id, vertical, accountable_entity_id, accountable_entity_type, activation_date, min_date_for_fur, city,

    SUM(april_2024) +
    SUM(may_2024) +
    SUM(june_2024) +
    SUM(july_2024) +
    SUM(august_2024) +
    SUM(september_2024) +
    SUM(october_2024) +
    SUM(november_2024) +
    SUM(december_2024) +
    SUM(january_2025) +
    SUM(february_2025) +
    SUM(march_2025) +
    SUM(april_2025) AS april_2025
FROM 
    other_discounts
GROUP BY 
    1,2,3,4,5,6,7
ORDER BY fur_id, accountable_entity_id
    
