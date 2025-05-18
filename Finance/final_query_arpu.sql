create materialized view final_query as 

--drop materialized view final_query

SELECT *
FROM (
    WITH cte_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type,  acquisition_type, item_ids,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN prices END) AS april_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN prices END) AS may_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN prices END) AS june_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN prices END) AS july_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN prices END) AS august_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN prices END) AS september_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN prices END) AS october_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN prices END) AS november_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN prices END) AS december_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN prices END) AS january_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN prices END) AS february_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN prices END) AS march_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN prices END) AS april_2025
          
        FROM 
            upsell
        WHERE 1=1 
        and vertical = 'FURLENCO_RENTAL'
        GROUP BY 
            1,2,3,4,5
    ),
    cte_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type,  acquisition_type, item_ids,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN prices END) AS april_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN prices END) AS may_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN prices END) AS june_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN prices END) AS july_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN prices END) AS august_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN prices END) AS september_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN prices END) AS october_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN prices END) AS november_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN prices END) AS december_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN prices END) AS january_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN prices END) AS february_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN prices END) AS march_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN prices END) AS april_2025

        FROM 
            upsell
        WHERE 1=1 
        and vertical = 'FURLENCO_RENTAL'
        GROUP BY 
            1,2,3,4,5
    ),
    vas_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type,  acquisition_type, item_ids,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN taxable_amount END) AS april_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN taxable_amount END) AS may_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN taxable_amount END) AS june_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN taxable_amount END) AS july_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN taxable_amount END) AS august_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN taxable_amount END) AS september_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN taxable_amount END) AS october_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN taxable_amount END) AS november_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN taxable_amount END) AS december_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN taxable_amount END) AS january_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN taxable_amount END) AS february_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN taxable_amount END) AS march_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN taxable_amount END) AS april_2025

        FROM 
            upsell
        WHERE 1=1 
        and vertical = 'FURLENCO_RENTAL'
 
        GROUP BY 
            1,2,3,4,5
    ),
    vas_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type,  acquisition_type, item_ids,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-04' THEN taxable_amount END) AS april_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-05' THEN taxable_amount END) AS may_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-06' THEN taxable_amount END) AS june_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-07' THEN taxable_amount END) AS july_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-08' THEN taxable_amount END) AS august_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-09' THEN taxable_amount END) AS september_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-10' THEN taxable_amount END) AS october_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-11' THEN taxable_amount END) AS november_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2024-12' THEN taxable_amount END) AS december_2024,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-01' THEN taxable_amount END) AS january_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-02' THEN taxable_amount END) AS february_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-03' THEN taxable_amount END) AS march_2025,
            SUM(CASE WHEN TO_CHAR(start_date, 'YYYY-MM') = '2025-04' THEN taxable_amount END) AS april_2025
        FROM 
            upsell
        WHERE 1=1 
        and vertical = 'FURLENCO_RENTAL'  

        GROUP BY 
            1,2,3,4,5
    ),
    final_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, item_ids,
            AVG(april_2024) AS april_2024,
            AVG(may_2024) AS may_2024,
            AVG(june_2024) AS june_2024,
            AVG(july_2024) AS july_2024,
            AVG(august_2024) AS august_2024,
            AVG(september_2024) AS september_2024,
            AVG(october_2024) AS october_2024,
            AVG(november_2024) AS november_2024,
            AVG(december_2024) AS december_2024,
            AVG(january_2025) AS january_2025,
            AVG(february_2025) AS february_2025,
            AVG(march_2025) AS march_2025,
            AVG(april_2025) AS april_2025,
            (acquisition_type || '_' || 'Base_Price') AS Price_type
        FROM 
            cte_new
        WHERE lower(accountable_entity_type) IN ('attachment', 'item')
        AND (acquisition_type || '_' || 'Base_Price') = 'New_Base_Price'
        GROUP BY 1, accountable_entity_id, accountable_entity_type, item_ids, acquisition_type
    ),
    final_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, item_ids,
            AVG(april_2024) AS april_2024,
            AVG(may_2024) AS may_2024,
            AVG(june_2024) AS june_2024,
            AVG(july_2024) AS july_2024,
            AVG(august_2024) AS august_2024,
            AVG(september_2024) AS september_2024,
            AVG(october_2024) AS october_2024,
            AVG(november_2024) AS november_2024,
            AVG(december_2024) AS december_2024,
            AVG(january_2025) AS january_2025,
            AVG(february_2025) AS february_2025,
            AVG(march_2025) AS march_2025,
            AVG(april_2025) AS april_2025,
            (acquisition_type || '_' || 'Base_Price') AS Price_type
        FROM 
            cte_upsell
        WHERE lower(accountable_entity_type) IN ('attachment', 'item')
        AND (acquisition_type || '_' || 'Base_Price') = 'Upsell_Base_Price'
        GROUP BY 1, accountable_entity_id, accountable_entity_type, item_ids, acquisition_type
    ),
    vas_data_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, item_ids,
            AVG(april_2024) AS april_2024,
            AVG(may_2024) AS may_2024,
            AVG(june_2024) AS june_2024,
            AVG(july_2024) AS july_2024,
            AVG(august_2024) AS august_2024,
            AVG(september_2024) AS september_2024,
            AVG(october_2024) AS october_2024,
            AVG(november_2024) AS november_2024,
            AVG(december_2024) AS december_2024,
            AVG(january_2025) AS january_2025,
            AVG(february_2025) AS february_2025,
            AVG(march_2025) AS march_2025,
            AVG(april_2025) AS april_2025,
            (acquisition_type || '_' || 'vas_price') AS Price_type
        FROM 
            vas_new
        WHERE lower(accountable_entity_type) IN ('value_added_service')
        GROUP BY 1, accountable_entity_id, accountable_entity_type, item_ids, acquisition_type
    ),
    vas_data_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, item_ids,
            AVG(april_2024) AS april_2024,
            AVG(may_2024) AS may_2024,
            AVG(june_2024) AS june_2024,
            AVG(july_2024) AS july_2024,
            AVG(august_2024) AS august_2024,
            AVG(september_2024) AS september_2024,
            AVG(october_2024) AS october_2024,
            AVG(november_2024) AS november_2024,
            AVG(december_2024) AS december_2024,
            AVG(january_2025) AS january_2025,
            AVG(february_2025) AS february_2025,
            AVG(march_2025) AS march_2025,
            AVG(april_2025) AS april_2025,
            (acquisition_type || '_' || 'vas_price') AS Price_type
        FROM 
            vas_upsell
        WHERE lower(accountable_entity_type) IN ('value_added_service')
        GROUP BY 1, accountable_entity_id, accountable_entity_type, item_ids, acquisition_type
    ),
    upfront_acq_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            '- Upfront_New_Discount' AS Price_type
        FROM kantas
        WHERE discounts = 'Upfront_acquisition_discounts-New'
          

        
    ),
    upfront_acq_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            '- Upfront_Upsell_Discount' AS Price_type
        FROM kantas
        WHERE discounts = 'Upfront_acquisition_discounts-Upsell'
          

        
    ),
    gross_arpu_new AS (
        SELECT 
            fl.fur_id, fl.accountable_entity_id, fl.accountable_entity_type, 
            (COALESCE(fl.april_2024, 0) - COALESCE(upn.april_2024, 0)) AS april_2024,
            (COALESCE(fl.may_2024, 0) - COALESCE(upn.may_2024, 0)) AS may_2024,
            (COALESCE(fl.june_2024, 0) - COALESCE(upn.june_2024, 0)) AS june_2024,
            (COALESCE(fl.july_2024, 0) - COALESCE(upn.july_2024, 0)) AS july_2024,
            (COALESCE(fl.august_2024, 0) - COALESCE(upn.august_2024, 0)) AS august_2024,
            (COALESCE(fl.september_2024, 0) - COALESCE(upn.september_2024, 0)) AS september_2024,
            (COALESCE(fl.october_2024, 0) - COALESCE(upn.october_2024, 0)) AS october_2024,
            (COALESCE(fl.november_2024, 0) - COALESCE(upn.november_2024, 0)) AS november_2024,
            (COALESCE(fl.december_2024, 0) - COALESCE(upn.december_2024, 0)) AS december_2024,
            (COALESCE(fl.january_2025, 0) - COALESCE(upn.january_2025, 0)) AS january_2025,
            (COALESCE(fl.february_2025, 0) - COALESCE(upn.february_2025, 0)) AS february_2025,
            (COALESCE(fl.march_2025, 0) - COALESCE(upn.march_2025, 0)) AS march_2025,
            (COALESCE(fl.april_2025, 0) - COALESCE(upn.april_2025, 0)) AS april_2025

        FROM 
            final_new AS fl 
        LEFT JOIN upfront_acq_new AS upn 
        ON fl.fur_id = upn.fur_id 
        AND fl.accountable_entity_id = upn.accountable_entity_id 
        AND fl.accountable_entity_type = upn.accountable_entity_type
    ),
    gross_arpu_upsell AS (
        SELECT 
            fl.fur_id, fl.accountable_entity_id, fl.accountable_entity_type, 
            (COALESCE(fl.april_2024, 0) - COALESCE(upn.april_2024, 0)) AS april_2024,
            (COALESCE(fl.may_2024, 0) - COALESCE(upn.may_2024, 0)) AS may_2024,
            (COALESCE(fl.june_2024, 0) - COALESCE(upn.june_2024, 0)) AS june_2024,
            (COALESCE(fl.july_2024, 0) - COALESCE(upn.july_2024, 0)) AS july_2024,
            (COALESCE(fl.august_2024, 0) - COALESCE(upn.august_2024, 0)) AS august_2024,
            (COALESCE(fl.september_2024, 0) - COALESCE(upn.september_2024, 0)) AS september_2024,
            (COALESCE(fl.october_2024, 0) - COALESCE(upn.october_2024, 0)) AS october_2024,
            (COALESCE(fl.november_2024, 0) - COALESCE(upn.november_2024, 0)) AS november_2024,
            (COALESCE(fl.december_2024, 0) - COALESCE(upn.december_2024, 0)) AS december_2024,
            (COALESCE(fl.january_2025, 0) - COALESCE(upn.january_2025, 0)) AS january_2025,
            (COALESCE(fl.february_2025, 0) - COALESCE(upn.february_2025, 0)) AS february_2025,
            (COALESCE(fl.march_2025, 0) - COALESCE(upn.march_2025, 0)) AS march_2025,
            (COALESCE(fl.april_2025, 0) - COALESCE(upn.april_2025, 0)) AS april_2025
    
        FROM 
            final_upsell AS fl 
        LEFT JOIN upfront_acq_upsell AS upn 
        ON fl.fur_id = upn.fur_id 
        AND fl.accountable_entity_id = upn.accountable_entity_id 
        AND fl.accountable_entity_type = upn.accountable_entity_type
    ),
    acq_new_data AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            '- New_Acquisition_Discount' AS Price_type
        FROM kantas
        WHERE discounts = 'Rent_Acquisition_Discounts-New'
        

        
    ),
    upsell_new_data AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            '- Upsell_Acquisition_Discount' AS Price_type
        FROM kantas
        WHERE discounts = 'Rent_Acquisition_Discounts-Upsell'
        

        
    ),
    auto_renewal_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'AUTO_RENEWAL_New' AS Price_type
        FROM kantas
        WHERE discounts = 'AUTO_RENEWAL-New'
          

        
    ),
    auto_renewal_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'AUTO_RENEWAL_Upsell' AS Price_type
        FROM kantas
        WHERE discounts = 'AUTO_RENEWAL-Upsell'
          

        
    ),
    upfront_renewal_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'Upfront_Renewal_New' AS Price_type
        FROM kantas
        WHERE discounts = 'Upfront_Renewal-New'
         

        
    ),
    upfront_renewal_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'Upfront_Renewal_Upsell' AS Price_type
        FROM kantas
        WHERE discounts = 'Upfront_Renewal-Upsell'
         

        
    ),
    retention_new AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'Retention_New' AS Price_type
        FROM kantas
        WHERE discounts = 'Retention-New'
         

        
    ),
    retention_upsell AS (
        SELECT 
            fur_id, accountable_entity_id, accountable_entity_type, 
            april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
            october_2024, november_2024, december_2024, january_2025, february_2025,
            march_2025, april_2025, 
            'Retention_Upsell' AS Price_type
        FROM kantas
        WHERE discounts = 'Retention-Upsell'
         

        
    ),
    vas_agg_new AS (
        SELECT 
            v.fur_id, i.accountable_entity_id, i.accountable_entity_type, 
            SUM(v.april_2024) AS april_2024,
            SUM(v.may_2024) AS may_2024,
            SUM(v.june_2024) AS june_2024,
            SUM(v.july_2024) AS july_2024,
            SUM(v.august_2024) AS august_2024,
            SUM(v.september_2024) AS september_2024,
            SUM(v.october_2024) AS october_2024,
            SUM(v.november_2024) AS november_2024,
            SUM(v.december_2024) AS december_2024,
            SUM(v.january_2025) AS january_2025,
            SUM(v.february_2025) AS february_2025,
            SUM(v.march_2025) AS march_2025,
            SUM(v.april_2025) AS april_2025,
      
            'Acquisition_VAS' AS Price_type
        FROM vas_data_new as v
        JOIN final_new i ON v.item_ids = i.accountable_entity_id
        WHERE v.Price_type = 'New_vas_price'
        GROUP BY v.fur_id, i.accountable_entity_id, i.accountable_entity_type, v.item_ids
    ),
    vas_agg_upsell AS (
        SELECT 
            v.fur_id, i.accountable_entity_id, i.accountable_entity_type, 
            SUM(v.april_2024) AS april_2024,
            SUM(v.may_2024) AS may_2024,
            SUM(v.june_2024) AS june_2024,
            SUM(v.july_2024) AS july_2024,
            SUM(v.august_2024) AS august_2024,
            SUM(v.september_2024) AS september_2024,
            SUM(v.october_2024) AS october_2024,
            SUM(v.november_2024) AS november_2024,
            SUM(v.december_2024) AS december_2024,
            SUM(v.january_2025) AS january_2025,
            SUM(v.february_2025) AS february_2025,
            SUM(v.march_2025) AS march_2025,
            SUM(v.april_2025) AS april_2025,
         
            'Upsell_VAS' AS Price_type
        FROM vas_data_upsell v
        JOIN final_upsell i ON v.item_ids = i.accountable_entity_id
        WHERE v.Price_type = 'Upsell_vas_price'
        GROUP BY v.fur_id, i.accountable_entity_id, i.accountable_entity_type, v.item_ids
    ),
    net_arpu_new AS (
        SELECT 
            g.fur_id, g.accountable_entity_id, g.accountable_entity_type, 
            (COALESCE(g.april_2024, 0) - COALESCE(a.april_2024, 0) - COALESCE(r.april_2024, 0) - COALESCE(ar.april_2024, 0) - COALESCE(rt.april_2024, 0) + COALESCE(v.april_2024, 0)) AS april_2024,
            (COALESCE(g.may_2024, 0) - COALESCE(a.may_2024, 0) - COALESCE(r.may_2024, 0) - COALESCE(ar.may_2024, 0) - COALESCE(rt.may_2024, 0) + COALESCE(v.may_2024, 0)) AS may_2024,
            (COALESCE(g.june_2024, 0) - COALESCE(a.june_2024, 0) - COALESCE(r.june_2024, 0) - COALESCE(ar.june_2024, 0) - COALESCE(rt.june_2024, 0) + COALESCE(v.june_2024, 0)) AS june_2024,
            (COALESCE(g.july_2024, 0) - COALESCE(a.july_2024, 0) - COALESCE(r.july_2024, 0) - COALESCE(ar.july_2024, 0) - COALESCE(rt.july_2024, 0) + COALESCE(v.july_2024, 0)) AS july_2024,
            (COALESCE(g.august_2024, 0) - COALESCE(a.august_2024, 0) - COALESCE(r.august_2024, 0) - COALESCE(ar.august_2024, 0) - COALESCE(rt.august_2024, 0) + COALESCE(v.august_2024, 0)) AS august_2024,
            (COALESCE(g.september_2024, 0) - COALESCE(a.september_2024, 0) - COALESCE(r.september_2024, 0) - COALESCE(ar.september_2024, 0) - COALESCE(rt.september_2024, 0) + COALESCE(v.september_2024, 0)) AS september_2024,
            (COALESCE(g.october_2024, 0) - COALESCE(a.october_2024, 0) - COALESCE(r.october_2024, 0) - COALESCE(ar.october_2024, 0) - COALESCE(rt.october_2024, 0) + COALESCE(v.october_2024, 0)) AS october_2024,
            (COALESCE(g.november_2024, 0) - COALESCE(a.november_2024, 0) - COALESCE(r.november_2024, 0) - COALESCE(ar.november_2024, 0) - COALESCE(rt.november_2024, 0) + COALESCE(v.november_2024, 0)) AS november_2024,
            (COALESCE(g.december_2024, 0) - COALESCE(a.december_2024, 0) - COALESCE(r.december_2024, 0) - COALESCE(ar.december_2024, 0) - COALESCE(rt.december_2024, 0) + COALESCE(v.december_2024, 0)) AS december_2024,
            (COALESCE(g.january_2025, 0) - COALESCE(a.january_2025, 0) - COALESCE(r.january_2025, 0) - COALESCE(ar.january_2025, 0) - COALESCE(rt.january_2025, 0) + COALESCE(v.january_2025, 0)) AS january_2025,
            (COALESCE(g.february_2025, 0) - COALESCE(a.february_2025, 0) - COALESCE(r.february_2025, 0) - COALESCE(ar.february_2025, 0) - COALESCE(rt.february_2025, 0) + COALESCE(v.february_2025, 0)) AS february_2025,
            (COALESCE(g.march_2025, 0) - COALESCE(a.march_2025, 0) - COALESCE(r.march_2025, 0) - COALESCE(ar.march_2025, 0) - COALESCE(rt.march_2025, 0) + COALESCE(v.march_2025, 0)) AS march_2025,
            (COALESCE(g.april_2025, 0) - COALESCE(a.april_2025, 0) - COALESCE(r.april_2025, 0) - COALESCE(ar.april_2025, 0) - COALESCE(rt.april_2025, 0) + COALESCE(v.april_2025, 0)) AS april_2025,
             
            'NET_ACQUISITION_ARPU_NEW_ORDER' AS Price_type
        FROM 
            gross_arpu_new g
        LEFT JOIN acq_new_data a 
        ON g.fur_id = a.fur_id 
        AND g.accountable_entity_id = a.accountable_entity_id 
        AND g.accountable_entity_type = a.accountable_entity_type
        LEFT JOIN upfront_renewal_new r 
        ON g.fur_id = r.fur_id 
        AND g.accountable_entity_id = r.accountable_entity_id 
        AND g.accountable_entity_type = r.accountable_entity_type
        LEFT JOIN auto_renewal_new ar 
        ON g.fur_id = ar.fur_id 
        AND g.accountable_entity_id = ar.accountable_entity_id 
        AND g.accountable_entity_type = ar.accountable_entity_type
        LEFT JOIN retention_new rt 
        ON g.fur_id = rt.fur_id 
        AND g.accountable_entity_id = rt.accountable_entity_id 
        AND g.accountable_entity_type = rt.accountable_entity_type
        LEFT JOIN vas_agg_new v 
        ON g.fur_id = v.fur_id 
        AND g.accountable_entity_id = v.accountable_entity_id 
        AND g.accountable_entity_type = v.accountable_entity_type
    ),
    net_arpu_upsell AS (
        SELECT 
            g.fur_id, g.accountable_entity_id, g.accountable_entity_type, 
            (COALESCE(g.april_2024, 0) - COALESCE(a.april_2024, 0) - COALESCE(r.april_2024, 0) - COALESCE(ar.april_2024, 0) - COALESCE(rt.april_2024, 0) + COALESCE(v.april_2024, 0)) AS april_2024,
            (COALESCE(g.may_2024, 0) - COALESCE(a.may_2024, 0) - COALESCE(r.may_2024, 0) - COALESCE(ar.may_2024, 0) - COALESCE(rt.may_2024, 0) + COALESCE(v.may_2024, 0)) AS may_2024,
            (COALESCE(g.june_2024, 0) - COALESCE(a.june_2024, 0) - COALESCE(r.june_2024, 0) - COALESCE(ar.june_2024, 0) - COALESCE(rt.june_2024, 0) + COALESCE(v.june_2024, 0)) AS june_2024,
            (COALESCE(g.july_2024, 0) - COALESCE(a.july_2024, 0) - COALESCE(r.july_2024, 0) - COALESCE(ar.july_2024, 0) - COALESCE(rt.july_2024, 0) + COALESCE(v.july_2024, 0)) AS july_2024,
            (COALESCE(g.august_2024, 0) - COALESCE(a.august_2024, 0) - COALESCE(r.august_2024, 0) - COALESCE(ar.august_2024, 0) - COALESCE(rt.august_2024, 0) + COALESCE(v.august_2024, 0)) AS august_2024,
            (COALESCE(g.september_2024, 0) - COALESCE(a.september_2024, 0) - COALESCE(r.september_2024, 0) - COALESCE(ar.september_2024, 0) - COALESCE(rt.september_2024, 0) + COALESCE(v.september_2024, 0)) AS september_2024,
            (COALESCE(g.october_2024, 0) - COALESCE(a.october_2024, 0) - COALESCE(r.october_2024, 0) - COALESCE(ar.october_2024, 0) - COALESCE(rt.october_2024, 0) + COALESCE(v.october_2024, 0)) AS october_2024,
            (COALESCE(g.november_2024, 0) - COALESCE(a.november_2024, 0) - COALESCE(r.november_2024, 0) - COALESCE(ar.november_2024, 0) - COALESCE(rt.november_2024, 0) + COALESCE(v.november_2024, 0)) AS november_2024,
            (COALESCE(g.december_2024, 0) - COALESCE(a.december_2024, 0) - COALESCE(r.december_2024, 0) - COALESCE(ar.december_2024, 0) - COALESCE(rt.december_2024, 0) + COALESCE(v.december_2024, 0)) AS december_2024,
            (COALESCE(g.january_2025, 0) - COALESCE(a.january_2025, 0) - COALESCE(r.january_2025, 0) - COALESCE(ar.january_2025, 0) - COALESCE(rt.january_2025, 0) + COALESCE(v.january_2025, 0)) AS january_2025,
            (COALESCE(g.february_2025, 0) - COALESCE(a.february_2025, 0) - COALESCE(r.february_2025, 0) - COALESCE(ar.february_2025, 0) - COALESCE(rt.february_2025, 0) + COALESCE(v.february_2025, 0)) AS february_2025,
            (COALESCE(g.march_2025, 0) - COALESCE(a.march_2025, 0) - COALESCE(r.march_2025, 0) - COALESCE(ar.march_2025, 0) - COALESCE(rt.march_2025, 0) + COALESCE(v.march_2025, 0)) AS march_2025,
            (COALESCE(g.april_2025, 0) - COALESCE(a.april_2025, 0) - COALESCE(r.april_2025, 0) - COALESCE(ar.april_2025, 0) - COALESCE(rt.april_2025, 0) + COALESCE(v.april_2025, 0)) AS april_2025,
          
            'NET_Upsell_ARPU' AS Price_type
        FROM 
            gross_arpu_upsell g
        LEFT JOIN upsell_new_data a 
        ON g.fur_id = a.fur_id 
        AND g.accountable_entity_id = a.accountable_entity_id 
        AND g.accountable_entity_type = a.accountable_entity_type
        LEFT JOIN upfront_renewal_upsell r 
        ON g.fur_id = r.fur_id 
        AND g.accountable_entity_id = r.accountable_entity_id 
        AND g.accountable_entity_type = r.accountable_entity_type
        LEFT JOIN auto_renewal_upsell ar 
        ON g.fur_id = ar.fur_id 
        AND g.accountable_entity_id = ar.accountable_entity_id 
        AND g.accountable_entity_type = ar.accountable_entity_type
        LEFT JOIN retention_upsell rt 
        ON g.fur_id = rt.fur_id 
        AND g.accountable_entity_id = rt.accountable_entity_id 
        AND g.accountable_entity_type = rt.accountable_entity_type
        LEFT JOIN vas_agg_upsell v 
        ON g.fur_id = v.fur_id 
        AND g.accountable_entity_id = v.accountable_entity_id 
        AND g.accountable_entity_type = v.accountable_entity_type
    )
    -- New Acquisition components
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM final_new 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM auto_renewal_new
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM upfront_renewal_new
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM retention_new 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM upfront_acq_new 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025, 
        'Gross_Acquisition_ARPU_New_Order' AS Price_type
    FROM gross_arpu_new
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM acq_new_data
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM vas_agg_new
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM net_arpu_new
    -- Upsell components
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM final_upsell 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM auto_renewal_upsell
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM upfront_renewal_upsell
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM retention_upsell 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM upfront_acq_upsell 
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025, 
        'Gross_Upsell' AS Price_type
    FROM gross_arpu_upsell
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM upsell_new_data
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM vas_agg_upsell
    UNION ALL 
    SELECT 
        fur_id, accountable_entity_id, accountable_entity_type, 
        april_2024, may_2024, june_2024, july_2024, august_2024, september_2024,
        october_2024, november_2024, december_2024, january_2025, february_2025,
        march_2025, april_2025,  Price_type
    FROM net_arpu_upsell
) a

