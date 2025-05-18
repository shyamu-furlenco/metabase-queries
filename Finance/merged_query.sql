
--drop table analytics.revenue cascade

-- revn 
CREATE OR REPLACE PROCEDURE revenues_proc() 
AS $$ 
BEGIN 
	DROP table IF EXISTS analytics.revenue;

	create table analytics.revenue AS  
(

with revn as 
 (
select distinct
    a.*,vas.name as vas_name,vas.state as vas_current_state,coalesce(i.hsn_code,atta.hsn_code) as hsn_code,
    CASE WHEN accountable_entity_type = 'ITEM' THEN i.activation_date 
        WHEN accountable_entity_type = 'PLAN' THEN pl.activation_date
        WHEN accountable_entity_type = 'ATTACHMENT' THEN atta.activation_date END as activation_date,
        MIN(a.start_date) over(partition by fur_id,accountable_entity_id) as min_date_for_fur,
    CASE WHEN accountable_entity_type = 'ITEM' THEN i.is_migrated_for_evolve 
        WHEN accountable_entity_type = 'PLAN' THEN pl.is_migrated_for_evolve
        WHEN accountable_entity_type = 'ATTACHMENT' THEN atta.is_migrated_for_evolve
        WHEN accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vas.is_migrated_for_evolve END as is_migrated_for_evolve,
        
    CASE WHEN accountable_entity_type = 'ITEM' THEN 
        CASE WHEN json_extract_path_text(i.pricing_details,'basePrice') <> '' THEN json_extract_path_text(i.pricing_details,'basePrice')::numeric(10,2) ELSE 0 END
    WHEN accountable_entity_type = 'PLAN' THEN 
        CASE WHEN json_extract_path_text(pl.pricing_details,'basePrice') <> '' THEN json_extract_path_text(pl.pricing_details,'basePrice')::numeric(10,2) ELSE 0 END
    WHEN accountable_entity_type = 'ATTACHMENT' THEN 
        CASE WHEN json_extract_path_text(atta.pricing_details,'basePrice') <> '' THEN json_extract_path_text(atta.pricing_details,'basePrice')::numeric(10,2) ELSE 0 END
    WHEN accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN 
        CASE WHEN json_extract_path_text(vas.pricing_details,'basePrice') <> '' THEN json_extract_path_text(vas.pricing_details,'basePrice')::numeric(10,2) ELSE 0 END
    END as base_price
 from 
 (select 
     fb.*
     ,coalesce(uledger.medium_identifier,mit.medium_identifier) as medium_identifier
     ,coalesce(ritem.created_at,rat.created_at,pc.created_at) as Return_requested_date
     ,coalesce(ritem.fulfillment_date,rat.fulfillment_date,pct.updated_at) as Return_completed_date
    
     ,case when vertical='FURLENCO_RENTAL' and npa.fur_id is not null then 'YES' else 'NO' end as NPA_CUSTOMER
 from analytics.fb_revenue fb
 left join
     (select a.id,trans_type,t.medium_identifier from 
     (select id,'ORDER' as trans_type,json_extract_path_text(payment_details,'id') payment_Id from order_management_systems_evolve.Orders
     union all
     select id,'RENEWAL' as trans_type,json_extract_path_text(payment_details,'id') payment_Id from order_management_systems_evolve.renewals) a 
     left join 
     (select payment_id,medium_identifier from gringotts_evolve.transactions where status = 2) t on t.payment_id = a.payment_Id) mit 
         on mit.id=fb.external_reference_id
         and mit.trans_type=fb.external_reference_type
     left join order_management_systems_evolve.return_items ritem 
     on ritem.item_id=fb.accountable_entity_id and fb.accountable_entity_type='ITEM' and ritem.state!='CANCELLED'
     left join order_management_systems_evolve.Plan_Cancellations pc
         on pc.plan_id=fb.accountable_entity_id and fb.accountable_entity_type='PLAN' and pc.state!='CANCELLED'
     left join order_management_systems_evolve.Plan_Cancellations pct
         on pct.plan_id=fb.accountable_entity_id and fb.accountable_entity_type='PLAN' and pct.state='COMPLETED'
     left join order_management_systems_evolve.return_attachments rat
         on rat.attachment_id=fb.accountable_entity_id and fb.accountable_entity_type='ATTACHMENT'  and rat.state!='CANCELLED'
     left join (select customer_plan_id,tenure_start_date,tenure_end_date,medium_identifier from analytics.unlmtd_ledger_V2) uledger 
         on uledger.customer_plan_id=fb.accountable_entity_id 
         and fb.accountable_entity_type='PLAN'
         AND (fb.start_date+ interval '330 minutes') >= uledger.tenure_start_date 
         AND (fb.end_date+ interval '330 minutes') >= uledger.tenure_start_date 
         AND (fb.end_date+ interval '330 minutes') <= uledger.tenure_end_date
     left join (select distinct fur_id from analytics.npa_customers)  npa 
         on npa.fur_id =fb.fur_id


 where 1=1 and end_date>='2024-04-01'
 ) a
 left join 
 order_management_systems_evolve.value_added_services vas 
     on a.accountable_entity_id=vas.id 
     and a.accountable_entity_type='VALUE_ADDED_SERVICE'
 left join order_management_systems_evolve.items i 
 on a.accountable_entity_id=i.id 
 and a.accountable_entity_type='ITEM'
 left join order_management_systems_evolve.attachments atta 
 on a.accountable_entity_id=atta.id 
 and a.accountable_entity_type='ATTACHMENT'
 LEFT JOIN order_management_systems_evolve.plans as pl 
 ON a.accountable_entity_id = pl.id and a.accountable_entity_type = 'PLAN'
 
 where  a.recognition_state not in ('CANCELLED','INVALIDATED') 
 and NPA_CUSTOMER='NO'

)

-- Base_



,base_ as (
SELECT  
    rn.*, rr.accountable_entity_id as rr_entity_id, rr.accountable_entity_type as rr_entity_type,
    rr.start_date as rr_start_date, rr.end_date as rr_end_date, rrs.id as rrs_id, 
    rrs.start_date as rrs_start_date, rrs.end_date rrs_end_date, rr.state as rr_state
 	
FROM 
    revn as rn 
  INNER JOIN furbooks_evolve.revenue_recognitions rr 
	  ON rr.accountable_entity_type = rn.accountable_entity_type
	  AND rr.accountable_entity_id = rn.accountable_entity_id
	  AND rr.start_date = rn.start_date
	  AND rr.end_date = rn.end_date
	  AND rr.state NOT IN ('CANCELLED', 'INVALIDATED')

  INNER JOIN furbooks_evolve.revenue_recognition_schedules rrs
  ON rrs.id = rr.revenue_recognition_schedule_id
  	  AND rrs.state NOT IN ('CANCELLED', 'INVALIDATED')

)


-- sms_base
,sms_base as
(

	SELECT 
		entity_id, entity_type, tenure_start_date, tenure_end_date, sms_base_price
		,rrs.start_date , rrs.end_date, rrs.id as rrs_id, tenures
	FROM 
		furbooks_evolve.revenue_recognition_schedules as rrs	
	LEFT JOIN
		 transitions as ts
	ON rrs.accountable_entity_id = ts.entity_id 
	and rrs.accountable_entity_type = ts.entity_type 
	and rrs.start_date = tenure_start_date::date 
	and rrs.end_date = tenure_end_date::date
--	and rrs.is_migrated = 'false'
	and rrs.state NOT IN ('CANCELLED', 'INVALIDATED')
)

-- modis as 

, modis as (
with coupons as (
WITH cte AS (
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
, joined_data AS (
    SELECT distinct
        cte.accountable_entity_id,
        cte.accountable_entity_type,
        cte.start_date, cte.end_date,
        ds.type AS discount_type,
--        replace(replace(exclusive_flows,'{',''),'}','') as exclusive_flows,
    CASE 
        WHEN cte.discount_amount IS NOT NULL AND cte.discount_amount != '' 
             AND cte.discount_amount ~ '^[0-9]+(\.[0-9]+)?$' 
        THEN cte.discount_amount::numeric(10,2)
        ELSE NULL
    END AS discount_amount,
        catalog_reference_id
    FROM 
        cte
    LEFT JOIN 
        godfather_evolve.Discounts AS ds
    ON 
        cte.catalog_reference_id = ds.id
)

SELECT 
    accountable_entity_id,
    accountable_entity_type,
    start_date, end_date,
--    exclusive_flows,
    SUM(CASE WHEN discount_type = 'COUPON_CODE' THEN discount_amount END) AS coupon_code, 
    SUM(CASE WHEN discount_type = 'UPFRONT' THEN discount_amount END) AS upfront,
    SUM(CASE WHEN discount_type = 'REFERRAL_PROMO' THEN discount_amount END) AS referral_promo,
    SUM(CASE WHEN discount_type = 'REFERRAL_CODE' THEN discount_amount END) AS referral_code,
    SUM(CASE WHEN discount_type = 'LOYALTY' THEN discount_amount END) AS loyalty,
    SUM(CASE WHEN catalog_reference_id = 0 THEN discount_amount END) as payment
FROM 
    joined_data
-- WHERE accountable_entity_id = 874945
GROUP BY 1,2,3,4
)

, base as (
SELECT distinct 
    b.fur_id, b.vertical, b.user_id, b.city, b.external_reference_type, b.external_reference_id, 
    b.accountable_entity_id, b.accountable_entity_type, b.current_state_product, taxableamount, charged_till_date, igst_rate, sgst_rate, cgst_rate, 
    
    b.start_date, b.end_date, b.recognition_state, b.recognised_at, b.to_be_recognised_on, b.recognition_type, b.total_tax, b.return_requested_date, b.return_completed_date,
    b.npa_customer, b.vas_name, b.vas_current_state, b.activation_date, b.min_date_for_fur, 
    

    b.base_price,
    s.sms_base_price,  tenure_start_date, tenure_end_date, entity_id, s.entity_type,
    CASE WHEN (b.accountable_entity_type <> 'PLAN' ) and s.sms_base_price::float isnull 
    THEN b.base_price::float ELSE sms_base_price::float END as prices,
    ROUND((rrs_end_date - rrs_start_date )/30.45) as tenure
--    COALESCE(tenureInMonths, Max(tenureInMonths) OVER (partition by accountable_entity_id )) AS tenure_in_months,
--    COALESCE(sms_base_price, Max(sms_base_price) OVER (partition by accountable_entity_id )) AS sms_base_price

FROM 
	base_ as b 
LEFT JOIN
    sms_base as s 
ON s.rrs_id = b.rrs_id
WHERE b.end_date>='2024-04-01' 
and rr_state not in ('CANCELLED', 'INVALIDATED')

)

SELECT distinct 
    b.*, coupon_code, upfront, referral_promo, referral_code, payment, loyalty --, exclusive_flows
FROM 
    base as b 
LEFT JOIN 
    coupons as c 
ON b.accountable_entity_id = c.accountable_entity_id and b.accountable_entity_type = c.accountable_entity_type 
AND b.start_date = c.start_date and b.end_date = c.end_date
)

-- Final Revenue

SELECT distinct mr.fur_Id, mr.vertical, mr.user_id, mr.city, mr.external_reference_type, mr.external_reference_id, mr.accountable_entity_id, mr.accountable_entity_type
        ,mr.current_state_product, mr.igst_rate, mr.sgst_rate, mr.cgst_rate, mr.recognition_state, mr.recognition_type, mr.total_tax, mr.npa_customer, mr.vas_name 
        ,mr.vas_current_state, mr.min_date_for_fur, mr.prices, 
CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN coupon_code*1.0/vp.tenures
     ELSE coupon_code end as coupon_code,
         
         
CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN upfront*1.0/vp.tenures
     ELSE upfront end as upfront,
                
CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN referral_promo*1.0/vp.tenures
     ELSE referral_promo end as referral_promo,
      

CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN referral_code*1.0/vp.tenures
     ELSE referral_code end as referral_code, 
     
       
CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN payment*1.0/vp.tenures
     ELSE payment end as payment,
     
 CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' AND COALESCE(vp.tenures,0) > 0 
     THEN loyalty*1.0/vp.tenures
     ELSE loyalty end as loyalty
   
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vas_taxable_amount::float ELSE taxableamount::float END as taxable_amount
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vp.charged_till_date ELSE mr.charged_till_date END as charged_till_date
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vp.activation_date ELSE mr.activation_date END as activation_date
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vp.start_date ELSE mr.start_date END as start_date
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vp.end_date ELSE mr.end_date END as end_date
        ,CASE WHEN mr.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vp.tenures ELSE mr.tenure END as tenures
from modis as mr 
LEFT JOIN vas_split as vp
ON vp.vas_id = mr.accountable_entity_id 
AND vp.accountable_entity_type = mr.accountable_entity_type


);
	END; 
	$$ LANGUAGE plpgsql;
	
	CALL revenues_proc()








