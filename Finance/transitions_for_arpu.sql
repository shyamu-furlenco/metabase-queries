CREATE MATERIALIZED VIEW transitions AUTO REFRESH  YES AS 
WITH base AS (
    SELECT DISTINCT 
        entity_id,
        entity_type,
        -- Extract basePrice safely
        CASE 
            WHEN "snapshot" LIKE '%"basePrice":"%' 
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"basePrice":"', 2), '"', 1) 
            ELSE NULL 
        END AS sms_base_price, 
        
        -- Extract tenureStartDate safely
        CASE 
            WHEN "snapshot" LIKE '%"tenureStartDate":"%' 
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"tenureStartDate":"', 2), '"', 1) 
            ELSE NULL 
        END AS tenure_start_date,

        -- Extract tenureEndDate safely
        CASE 
            WHEN "snapshot" LIKE '%"tenureEndDate":"%' 
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"tenureEndDate":"', 2), '"', 1) 
            ELSE NULL 
        END AS tenure_end_date,

        -- Extract tenureInMonths using regex to capture only digits
        CASE 
            WHEN "snapshot" LIKE '%"tenureInMonths":%' 
            THEN REGEXP_SUBSTR("snapshot", '"tenureInMonths":([0-9]+)', 1, 1, 'e')
            ELSE NULL 
        END AS tenureinmonths_text,

        -- Extract tenureInMonths using split_part (fallback method)
        CASE 
            WHEN "snapshot" LIKE '%"tenureInMonths":%'         
        	THEN SPLIT_PART(SPLIT_PART("snapshot", '"tenureInMonths":', 2), ',', 1)
        	ELSE NULL 
        END as tenures,

        -- Extract entity_states
        CASE 
            WHEN "snapshot" LIKE '%"state":"%'
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"state":"', 2), '"', 1) 
            ELSE NULL 
        END AS entity_states,

        -- Extract renewalOverdueCycleStartDate
        CASE 
            WHEN "snapshot" LIKE '%"renewalOverdueCycleStartDate":"%'
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"renewalOverdueCycleStartDate":"', 2), '"', 1)
            ELSE NULL 
        END AS renewalOverdueCycleStartDate,

        -- Extract renewalOverdueCycleEndDate
        CASE 
            WHEN "snapshot" LIKE '%"renewalOverdueCycleEndDate":"%'
            THEN SPLIT_PART(SPLIT_PART("snapshot", '"renewalOverdueCycleEndDate":"', 2), '"', 1)
            ELSE NULL 
        END AS renewalOverdueCycleEndDate
    FROM order_management_systems_evolve.State_Transitions AS st
    WHERE st.entity_type IN ('ITEM', 'ATTACHMENT', 'PLAN') 
        AND to_state IN ('ACTIVE', 'ACTIVATED', 'RENEWAL_OVERDUE', 'PICKUP_TO_BE_SCHEDULED','RENTAL_PRODUCT_OVERDUE_FOR_RENEWAL')
)
, final as (
	SELECT 
	    entity_id, 
	    entity_type, 
	    sms_base_price, 
	    entity_states,
	    
	    -- Handle tenure start date
	    CASE 
	        WHEN entity_states = 'ACTIVE' THEN tenure_start_date
	        WHEN entity_states = 'ACTIVATED' THEN tenure_start_date
	        WHEN entity_states = 'RENEWAL_OVERDUE' THEN renewaloverduecyclestartdate
	         ELSE NULL 
	    END AS tenure_start_date, 
	        
	    -- Handle tenure end date
	    CASE 
	        WHEN entity_states = 'ACTIVE' THEN tenure_end_date
	        WHEN entity_states = 'ACTIVATED' THEN tenure_end_date
	        WHEN entity_states = 'RENEWAL_OVERDUE' THEN renewaloverduecycleenddate
	        ELSE NULL 
	    END AS tenure_end_date, 
	        
	    -- Handle tenureInMonths safely
	    CASE 
	        WHEN entity_states = 'ACTIVE' AND tenureinmonths_text ~ '^[0-9]+$' 
	        THEN tenureinmonths_text::INT
	        WHEN entity_states = 'ACTIVATED' AND tenureinmonths_text ~ '^[0-9]+$' 
	        THEN tenureinmonths_text::INT
	        
	        WHEN entity_states = 'RENEWAL_OVERDUE' 
	             AND renewaloverduecyclestartdate ~ '^\d{4}-\d{2}-\d{2}$'
	             AND renewaloverduecycleenddate ~ '^\d{4}-\d{2}-\d{2}$'
	             AND renewaloverduecyclestartdate IS NOT NULL 
	             AND renewaloverduecyclestartdate <> ''
	             AND length(renewaloverduecyclestartdate) = 10
	             AND renewaloverduecycleenddate IS NOT NULL 
	             AND renewaloverduecycleenddate <> ''
	             AND length(renewaloverduecycleenddate) = 10
	        THEN ROUND((DATE(renewaloverduecycleenddate) - DATE(renewaloverduecyclestartdate)) / 30.60)
	        
	        ELSE NULL 
	    END AS tenures 
	
	FROM base
)
SELECT 
		entity_id, 
	    entity_type, 
	    sms_base_price, 
	    entity_states,
	    CASE WHEN length(tenure_start_date) = 10 THEN tenure_start_date::date END as tenure_start_date,
	    CASE WHEN length(tenure_end_date) = 10 THEN tenure_end_date::date END as tenure_end_date, 
	    tenures
FROM 
	final
	
