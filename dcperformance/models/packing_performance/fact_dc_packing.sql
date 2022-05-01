
{{ config(materialized='table') }}


WITH cte_1 AS (
	SELECT 
		user_id,
                action_code,
                order_number,
                item_number,
                sum(quantity) as qty_packed,
		MAX(timestamp_time) as max_ts
	FROM 
		dcperformance.stg_transactions
	WHERE 
		user_id= 'USER13'
		group by user_id, action_code, order_number, item_number
		order by max_ts asc)
		
	, cte_2 AS (
	SELECT 
	user_id,
  action_code,
  item_number,
  order_number,
  qty_packed,
  max_ts,
  LAG(max_ts,1)
		OVER (PARTITION BY order_number ORDER BY max_ts) previous_ts
FROM 
	cte_1)
	
SELECT 
	order_number,
	user_id, 
	action_code, 
	item_number,
	qty_packed,
	max_ts,
	previous_ts,  
	(max_ts - previous_ts) as time_since_last_pack
FROM 
	cte_2
	

