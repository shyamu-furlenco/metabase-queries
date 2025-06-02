--drop materialized view par 

create materialized view par as

WITH
  base AS (
    SELECT
      items.id AS item_id,
      activation_date,
      user_id,
      CASE
        WHEN return_items.created_at IS NULL
        AND rent_to_purchase_orders.created_at IS NULL THEN NULL
        WHEN rent_to_purchase_orders.created_at IS NULL THEN return_id
        WHEN return_items.created_at IS NULL THEN rent_to_purchase_order_id
      END AS Transaction_id,
      CASE
        WHEN return_items.created_at IS NULL
        AND rent_to_purchase_orders.created_at IS NULL THEN CURRENT_DATE + 1
        WHEN rent_to_purchase_orders.created_at IS NULL THEN return_items.created_at + interval '330 minutes'
        WHEN return_items.created_at IS NULL THEN rent_to_purchase_orders.created_at + interval '330 minutes'
      END AS item_transaction_date,
      NULLIF(
        json_extract_path_text(
          rent_to_purchase_items.payment_details,
          'payableAfterPaymentOffers',
          'byCashPreTax'
        ),
        ''
      ) AS tto_pay,
      NULLIF(
        json_extract_path_text(
          json_extract_array_element_text(rent_to_purchase_items.offers_snapshot, 1),
          'code'
        ),
        ''
      ) AS code_2,
      NULLIF(
        json_extract_path_text(
          json_extract_array_element_text(rent_to_purchase_items.offers_snapshot, 1),
          'discountAmount'
        ),
        ''
      )::float AS code_2_discount,
      CASE
        WHEN return_items.created_at IS NULL
        AND rent_to_purchase_orders.created_at IS NULL THEN NULL
        WHEN rent_to_purchase_orders.created_at IS NULL THEN 'return_item'
        WHEN return_items.created_at IS NULL THEN 'rent_to_purchase_item'
      END AS Transaction_type
    FROM
      order_management_systems_evolve.items
      LEFT JOIN order_management_systems_evolve.return_items ON items.id = return_items.item_id
      AND return_items.state != 'CANCELLED'
      LEFT JOIN order_management_systems_evolve.rent_to_purchase_items ON rent_to_purchase_items.item_id = items.id
      AND POSITION(
        'paid' IN LOWER(rent_to_purchase_items.payment_details)
      ) > 0
      LEFT JOIN order_management_systems_evolve.rent_to_purchase_orders ON rent_to_purchase_orders.id = rent_to_purchase_items.rent_to_purchase_order_id
    WHERE
      items.vertical = 'FURLENCO_RENTAL'
      AND items.state != 'CANCELLED'
  ),
  
  
  
  
  base1 AS (
    SELECT
      base.item_id AS base_item_id,
      base.activation_date AS base_activation_date,
      base.user_id AS base_user_id,
      base.Transaction_id AS base_transaction_id,
      base.item_transaction_date AS base_item_transaction_date,
      base.Transaction_type AS base_transaction_type,
      dummy.*
      -- base.Transaction_id, base.item_transaction_date, sum(case when dummy.activation_date < dummy.item_transaction_date and dummy.item_transaction_date >= base.item_transaction_date then 1 else 0 end) as VAR1, count(distinct base.item_id) as VAR2
    FROM
      base
      LEFT JOIN (
        SELECT
          *
        FROM
          base
      ) AS dummy ON dummy.user_id = base.user_id
      AND dummy.activation_date < base.item_transaction_date
    WHERE
      base.Transaction_id IS NOT NULL
      -- group by 1,2
  ),
  
  
  
  fullorpartial AS (
    SELECT DISTINCT
      base_transaction_id,
      base_user_id,
      base_Transaction_type,
      CASE
        WHEN count(DISTINCT base_item_id) = count(
          DISTINCT CASE
            WHEN item_transaction_date >= base_item_transaction_date THEN item_id
            ELSE NULL
          END
        ) THEN 'FULL'
        ELSE 'PARTIAL'
      END AS VAR1
    FROM
      base1
    GROUP BY
      1, 2, 3
    ORDER BY
      1 DESC
  )
  
SELECT DISTINCT
  base.item_id,
  i.name AS item_name,
  json_extract_path_text(i.pricing_details, 'basePrice') AS base_price,
  i.user_id,
  json_extract_path_text(i.user_details, 'displayId') AS fur_id,
  i.activation_date + interval '330 minutes' AS activation_date,
  i.pickup_date + interval '330 minutes' AS pickup_date,
  base.item_transaction_date AS payment_date,
  date (CURRENT_DATE) AS charge_till_date,
  base.item_transaction_date AS applicable_on,
  tto_pay::float AS "TTO_amount",
  base.Transaction_type AS "Transaction_type",
  Transaction_id,
  VAR1 AS "Transaction_type_detail"
FROM
  base
  LEFT JOIN fullorpartial ON fullorpartial.base_transaction_id = base.Transaction_id
  AND base.Transaction_type = base_Transaction_type
  LEFT JOIN order_management_systems_evolve.items i ON i.id = base.item_id
WHERE
  Transaction_id IS NOT NULL
 
