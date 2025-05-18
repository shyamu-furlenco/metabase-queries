/*

Requesting Team : Cx Team
Requested By: Sandeep Babu
Requirement : To modify pre-existing query and create new one for customers whose shipment is delivered/picked_up and find the max reschedule count 
Slack thread / Jira ticket :
Pre-existing_query : https://metabase.furlenco.com/question/8414-post-hs-backlog-for-customer-calling-v1-0?contact_no=&furId=
Update Logs : 

*/


WITH
  base AS (
    SELECT
      fc_name,
      logistic_zone_n,
      json_extract_path_text(sch.user_details, 'displayId') AS customer_identifier,
      json_extract_path_text(sch.user_details, 'phoneNumber') AS customer_contact,
      json_extract_path_text(sch.user_details, 'name') AS customer_name,
      sch.vertical,
      sch.type AS schedule_type,
      sch.user_action_type,
      sch.user_action_display_id,
      sch.id AS schedule_id,
      sch.state schedule_state,
      sch.user_selected_fulfillment_date,
      sch.selected_fulfillment_date,
      sum(
        CASE
          WHEN sh.type = 'DELIVERY' THEN volume_in_cft
        END
      ) AS delivery_volume,
      sum(
        CASE
          WHEN sh.type = 'PICKUP' THEN volume_in_cft
        END
      ) AS pickup_volume,
      sum(sh.service_time_in_minutes) AS service_time,
      json_extract_path_text(
        sh.temporal_requirement_details,
        'timeToDoorstepPostArrivalInMinutes'
      )::int AS fixed_time,
      address,
      follow_up_date,
      sh.state as shipment_state, 
      is_priority,
      ancestor_schedule_ids,
      
      address_validation_status,
      date (sh.created_at + interval '330 minutes') AS shipment_creation_date
      ,MAX(sh.reschedule_count) AS max_reschedules
    FROM
      logistic_system_lms_evolve.shipments sh
      INNER JOIN logistic_system_lms_evolve.schedules sch ON sch.id = sh.schedule_id
      LEFT JOIN logistic_system_lms_evolve.user_address ua ON ua.id = sch.user_address_id
      LEFT JOIN (
        SELECT
          id,
          name AS logistic_zone_n
        FROM
          logistic_system_garuda_evolve.logistics_zones
      ) lgz ON lgz.id = sch.logistics_zone_id
      LEFT JOIN (
        SELECT
          id,
          name AS fc_name
        FROM
          logistic_system_garuda_evolve.fulfilment_centers
      ) fc ON fc.id = sch.fc_id
      LEFT JOIN (
        SELECT
          display_id,
          address_validation_status
        FROM
          logistic_system_garuda_evolve.shipments
      ) sg ON sg.display_id = sh.display_id
    WHERE 1=1
    --   sch.state NOT IN ('CANCELLED', 'PLANNED')
     AND (( sh.state in ('DELIVERED','DELIVERED_WITH_ISSUES')
      AND sh.type = 'DELIVERY') OR (sh.state in ('PICKED_UP','PICKED_UP_WITH_ISSUES','RETURNED_TO_FC') AND sh.type = 'PICKUP'))
    GROUP BY
        ALL
    ORDER BY
      1, 2, 3, 6,9, 10 
  )
SELECT
  *
FROM
  base
WHERE
  1 = 1 [[and customer_contact = {{contact_no}}]] [[and customer_identifier = {{furId}}]]