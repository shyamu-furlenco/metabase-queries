create materialized view kantas as 

--drop materialized view kantas cascade

--drop materialized view kantas

WITH coupon_base AS (
    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
        external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '0', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts', '0', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') --and rr.recognition_type = 'DEFERRAL'

    UNION ALL

    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
        external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '1', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts', '1', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') --and rr.recognition_type = 'DEFERRAL'

    UNION ALL

    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
        external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '2', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts', '2', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') --and rr.recognition_type = 'DEFERRAL'

    UNION ALL

    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
        external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '3', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts','3', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') -- and rr.recognition_type = 'DEFERRAL'
    
    UNION ALL

    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
		external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '4', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts', '4', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') --and rr.recognition_type = 'DEFERRAL'

    UNION ALL

    SELECT 
        rr.accountable_entity_id,
        rr.accountable_entity_type,
        start_date, end_date,
		external_reference_type,
        json_extract_path_text(monetary_components, 'discounts', '5', 'amount') AS discount_amount,
        json_extract_path_text(monetary_components, 'discounts', '5', 'catalogReferenceId') AS catalog_reference_id
    FROM 
        furbooks_evolve.Revenue_Recognitions AS rr
    WHERE rr.state in ('PROCESSED','FUTURE') --and rr.recognition_type = 'DEFERRAL'

)
, cpn as (
    SELECT distinct
        cb.accountable_entity_id,
        cb.accountable_entity_type,
        cb.start_date as starts_date, cb.end_date as ends_date,
        (trim (both '{}' from exclusive_flows)) as exl_flow,
    	(trim (both '{}' from order_types)) as order_type,
    	type,

    CASE 
        WHEN cb.discount_amount IS NOT NULL AND cb.discount_amount != '' 
             AND cb.discount_amount ~ '^[0-9]+(\.[0-9]+)?$' 
        THEN cb.discount_amount::numeric(10,2)
        ELSE NULL
    END AS discount_amount,
        catalog_reference_id
    FROM 
        coupon_base as cb
    LEFT JOIN 
        godfather_evolve.Discounts AS ds
    ON 
        cb.catalog_reference_id = ds.id
)


	, vas_link_with_item as (
	SELECT distinct r.*, 
	    COALESCE(CASE WHEN r.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vas.entity_id
	        WHEN r.accountable_entity_type = 'ATTACHMENT' THEN it.id END,r.accountable_entity_id) as item_ids
	FROM 
	    cpn as r 
	LEFT JOIN 
	    order_management_systems_evolve.Value_Added_Services as vas
	ON r.accountable_entity_id = vas.id
	AND accountable_entity_type = 'VALUE_ADDED_SERVICE'
	    LEFT JOIN order_management_systems_evolve.Attachments as at 
	    ON r.accountable_entity_id = at.id 
	    AND r.accountable_entity_type = 'ATTACHMENT'
	    LEFT JOIN order_management_systems_evolve.items as it 
	    ON at.composite_item_id = it.composite_item_id
	
	)
	,final as (
	SELECT distinct c.*, up.acquisition_type, up.external_reference_type
	FROM 
	    vas_link_with_item as c 
	LEFT JOIN 
	    upsell as up 
	ON c.item_ids = up.item_ids
	and c.starts_date = up.start_date and c.ends_date = up.end_date
	)
	, cps as (
	SELECT 
	    accountable_entity_id, accountable_entity_type , starts_date as start_date, ends_date as end_date,exl_flow, discount_amount,acquisition_type, external_reference_type,
	    CASE WHEN exl_flow = 'RENT_ACQUISITION' and type = 'UPFRONT' THEN 'Upfront_acquisition_discounts' 
	        WHEN exl_flow = 'RENT_ACQUISITION' and type in ('REFERRAL_CODE','COUPON_CODE') THEN 'Rent_Acquisition_Discounts'
	        WHEN exl_flow = 'ACQUISITION' and type in ('REFERRAL_PROMO','PAYMENT') THEN 'Acquisition_Discounts'
	        WHEN exl_flow = 'AUTO_RENEWAL' THEN 'AUTO_RENEWAL' 
	        WHEN exl_flow = 'RENEWAL' and type = 'UPFRONT' THEN 'Upfront_Renewal'
	        WHEN exl_flow = 'RENEWAL' and type = 'COUPON_CODE' THEN 'Retention' 
	        WHEN discount_amount is not null THEN 'NCEMI_Discounts' END || '-' || acquisition_type as discounts
	FROM final
	)


, cte as (
SELECT distinct cp.*, rn.fur_id, item_ids
FROM cps as cp
JOIN upsell as rn 
ON cp.accountable_entity_id = rn.accountable_entity_id and 
    cp.accountable_entity_type = rn.accountable_entity_type
)

SELECT 
    fur_id, accountable_entity_id, accountable_entity_type, discounts, item_ids, 
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN discount_amount END) AS april_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN discount_amount END) AS may_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN discount_amount END) AS june_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN discount_amount END) AS july_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN discount_amount END) AS august_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN discount_amount END) AS september_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN discount_amount END) AS october_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN discount_amount END) AS november_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN discount_amount END) AS december_2024,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN discount_amount END) AS january_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN discount_amount END) AS february_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN discount_amount END) AS march_2025,
    SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN discount_amount END) AS april_2025
    
FROM 
     cte
WHERE accountable_entity_type <> 'PLAN'
GROUP BY 
    1,2,3,4,5
