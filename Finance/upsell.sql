

create materialized view upsell as 

-- upsell logic

with upsell as (

WITH base AS (
  SELECT
    items.id AS item_id,
    items.order_id,
    json_extract_path_text(items.user_details,'displayId') as fur_Id,
    items.user_id,
    items.activation_date,
    items.created_at AS item_created_at,
   
    MIN(CASE WHEN return_items.state <> 'CANCELLED' THEN return_items.created_at END) AS return_date,
    MIN(CASE WHEN rtp_orders.state NOT IN ('CANCELLED') THEN rtp_orders.created_at END) AS rent_to_purchase_date
  FROM
    order_management_systems_evolve.items AS items
  LEFT JOIN 
    order_management_systems_evolve.return_items AS return_items 
    ON items.id = return_items.item_id
    AND return_items.state <> 'CANCELLED'
  LEFT JOIN
    order_management_systems_evolve.rent_to_purchase_items AS rtp_items
    ON items.id = rtp_items.item_id
  LEFT JOIN
    order_management_systems_evolve.rent_to_purchase_orders AS rtp_orders
    ON rtp_items.rent_to_purchase_order_id = rtp_orders.id
    AND rtp_orders.state NOT IN ('CANCELLED')
  WHERE
    items.vertical = 'FURLENCO_RENTAL'
    AND items.state NOT IN ('CANCELLED')
    AND items.activation_date IS NOT NULL
--    [[AND json_extract_path_text(items.user_details,'displayId') = {{fur_id}}]]
  GROUP BY
    items.id, items.order_id,
    json_extract_path_text(items.user_details,'displayId'),
    items.user_id, items.activation_date,
    items.created_at
),

new_acquisitions AS (
  SELECT
    user_id, item_id, order_id,
    fur_id, activation_date, item_created_at,
    COALESCE(return_date, rent_to_purchase_date) AS termination_date,
    DENSE_RANK() OVER(PARTITION BY user_id ORDER BY order_id) AS rnk
  FROM
    base
)

,final AS (
  SELECT 
    *,
    MAX(termination_date) OVER(
      PARTITION BY user_id 
      ORDER BY order_id, termination_date DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_termination_date,
    
    -- Correlated subquery to check for any active items in previous orders
    CASE WHEN EXISTS (
      SELECT 1 
      FROM base b
      WHERE b.user_id = new_acquisitions.user_id
        AND b.order_id < new_acquisitions.order_id
        AND COALESCE(b.return_date, b.rent_to_purchase_date) IS NULL
    ) THEN 1 ELSE 0 END AS has_active_previous_items_corr
  FROM
    new_acquisitions
)

SELECT
  fur_id, order_id, item_id, activation_date, 
  item_created_at, termination_date,
  has_active_previous_items_corr,
  MIN(previous_termination_date) OVER(PARTITION BY user_id, order_id) AS max_previous_termination_date
  
  ,CASE WHEN ((item_created_at > max_previous_termination_date and max_previous_termination_date is not null and has_active_previous_items_corr = 0) OR rnk = 1) 
  THEN 'New' ELSE 'Upsell' END as acquisition_type
  
FROM final
)


--joining queries

, cte as (
SELECT r.*, 
    COALESCE(CASE WHEN r.accountable_entity_type = 'VALUE_ADDED_SERVICE' THEN vas.entity_id
        WHEN r.accountable_entity_type = 'ATTACHMENT' THEN it.id END,r.accountable_entity_id) as item_ids
FROM 
    analytics.revenue as r 
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

SELECT c.*, up.acquisition_type
FROM 
    cte as c 
LEFT JOIN 
    upsell as up 
ON c.item_ids = up.item_id
