create materialized view vas_split AUTO REFRESH YES as 
with vas_month_split as (
-- items

with items as (
    with base as (
        SELECT vas.id as vas_Id, vas.type as vas_type,
              ROUND((vas.end_date - vas.start_date) / 30.60) as tenures,
              vas.entity_id, rr.start_date, rr.end_date
              
        FROM order_management_systems_evolve.Value_Added_Services AS vas
        JOIN furbooks_evolve.Revenue_Recognitions as rr
        ON vas.entity_id = rr.accountable_entity_id
        AND vas.entity_type = rr.accountable_entity_type
        WHERE vas.entity_type = 'ITEM' 
        AND vas.start_date <= rr.start_date
        AND vas.end_date >= rr.end_date
        AND vas.type in ('FURLENCO_CARE_PROGRAM', 'UNLMTD_FURLENCO_CARE_PROGRAM', 'UNLMTD_SWAP')
    )
    SELECT DISTINCT vas_id, vas_type, tenures, entity_id, rr.accountable_entity_type, 
          (json_extract_path_text(monetary_components, 'taxableAmount')::float/tenures::float) as vas_taxable_amount,
          rr.state, b.start_date, b.end_date
    FROM base as b
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON b.vas_id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    
    UNION ALL
    
    SELECT vas.id as vas_Id, vas.type, '0'::float as tenures, entity_id, rr.accountable_entity_type,
          json_extract_path_text(monetary_components, 'taxableAmount')::float as taxableAmount,
          rr.state, rr.start_date, rr.end_date
    FROM order_management_systems_evolve.Value_Added_Services AS vas
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON vas.id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    AND vas.entity_type = 'ITEM'
    AND vas.type in ('DELIVERY_CHARGE', 'FLEXI_CANCELLATION', 'AC_INSTALLATION_CHARGE', 'UNLMTD_FLEXI_CANCELLATION')
)

-- plans
, plans as (
    with base as (
        SELECT vas.id as vas_Id, vas.type as vas_type,
              ROUND((vas.end_date - vas.start_date) / 30.60)::float as tenures,
              vas.entity_id,  rr.start_date, rr.end_date
        FROM order_management_systems_evolve.Value_Added_Services AS vas
        JOIN furbooks_evolve.Revenue_Recognitions as rr
        ON vas.entity_id = rr.accountable_entity_id
        AND vas.entity_type = rr.accountable_entity_type
        WHERE vas.entity_type = 'PLAN' 
        AND vas.start_date <= rr.start_date
        AND vas.end_date >= rr.end_date
        AND vas.type in ('FURLENCO_CARE_PROGRAM', 'UNLMTD_FURLENCO_CARE_PROGRAM', 'UNLMTD_SWAP')
    )
    SELECT DISTINCT vas_id, vas_type, tenures, entity_id, rr.accountable_entity_type, 
          (json_extract_path_text(monetary_components, 'taxableAmount')::float/tenures) as vas_taxable_amount,
          rr.state, b.start_date, b.end_date
    FROM base as b
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON b.vas_id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    
    UNION ALL
    
    SELECT vas.id as vas_Id, vas.type, '0'::float as tenures, entity_id, rr.accountable_entity_type,
          json_extract_path_text(monetary_components, 'taxableAmount')::float as taxableAmount,
          rr.state, rr.start_date, rr.end_date
    FROM order_management_systems_evolve.Value_Added_Services AS vas
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON vas.id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    AND vas.entity_type = 'PLAN'
    AND vas.type in ('DELIVERY_CHARGE', 'FLEXI_CANCELLATION', 'AC_INSTALLATION_CHARGE', 'UNLMTD_FLEXI_CANCELLATION')
)

-- attachments
, attachments as (
    with base as (
        SELECT vas.id as vas_Id, vas.type as vas_type,
              ROUND((vas.end_date - vas.start_date) / 30.60) as tenures,
              vas.entity_id,rr.start_date, rr.end_date
        FROM order_management_systems_evolve.Value_Added_Services AS vas
        JOIN furbooks_evolve.Revenue_Recognitions as rr
        ON vas.entity_id = rr.accountable_entity_id
        AND vas.entity_type = rr.accountable_entity_type
        WHERE vas.entity_type = 'ATTACHMENT' 
        AND vas.start_date <= rr.start_date
        AND vas.end_date >= rr.end_date
        AND vas.type in ('FURLENCO_CARE_PROGRAM', 'UNLMTD_FURLENCO_CARE_PROGRAM', 'UNLMTD_SWAP')
    )
    SELECT DISTINCT vas_id, vas_type, tenures, entity_id, rr.accountable_entity_type, 
          (json_extract_path_text(monetary_components, 'taxableAmount')::float/tenures::float) as vas_taxable_amount,
          rr.state, b.start_date, b.end_date
    FROM base as b
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON b.vas_id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    
    UNION ALL
    
    SELECT vas.id as vas_Id, vas.type, '0'::float as tenures, entity_id, rr.accountable_entity_type,
          json_extract_path_text(monetary_components, 'taxableAmount')::float as taxableAmount,
          rr.state, rr.start_date, rr.end_date
    FROM order_management_systems_evolve.Value_Added_Services AS vas
    JOIN furbooks_evolve.Revenue_Recognitions as rr
    ON vas.id = rr.accountable_entity_id
    WHERE rr.accountable_entity_type = 'VALUE_ADDED_SERVICE'
    AND vas.entity_type = 'ATTACHMENT'
    AND vas.type in ('DELIVERY_CHARGE', 'FLEXI_CANCELLATION', 'AC_INSTALLATION_CHARGE', 'UNLMTD_FLEXI_CANCELLATION')
)

SELECT * FROM items
UNION ALL 
SELECT * FROM plans
UNION ALL 
SELECT * FROM attachments

)

SELECT *, min(start_date) over(partition by vas_id) as activation_date,
max(end_date) over(partition by vas_id) as charged_till_date 
FROM vas_month_split
