
SELECT date,
ifnull(p.name, 'Organic') AS Partner,
ifnull(device_type, 'Unknown') AS Device_Type,
ifnull(device_carrier, 'Unknown') AS Device_Carrier,
sum(costs) AS costs,
sum(clicks) AS click_count,
sum(installs) AS install_count,
sum(events) AS event_count,
sum(opens) AS open_count,
sum(revenues_usd) AS revenues_usd
FROM (
  SELECT date(log_created) as date,
  publisher_id,
  device_type,
        0 as device_carrier,
  sum(costs) as costs,
  sum(clicks) as clicks,
  0 as installs,
  0 as events,
  0 as revenues_usd,
        0 as opens
  FROM log_attributables
  WHERE advertiser_id = 3444
  and log_created >= '2015-10-01'
  and log_created < date_add('2015-11-30', INTERVAL 1 DAY)
  and debug_mode = 0 and test_profile_id = 0
  and log_type = 'click'
  GROUP BY 1,2,3,4
   UNION ALL
  SELECT date(log_created) as date,
  if(reengagement_publisher_id = 0, install_publisher_id, reengagement_publisher_id) as publisher_id,
  device_type,
        device_carrier,
  sum(costs) as costs,
  0 as clicks,
  sum(installs) as installs,
  sum(events) as events,
        sum(opens) as opens,
  sum(revenues_usd) AS revenues_usd
  FROM log_conversions
  WHERE advertiser_id = 3444
  and log_created >= '2015-10-01'
  and log_created < date_add('2015-11-30', INTERVAL 1 DAY)
  and debug_mode = 0 and test_profile_id = 0 and log_status = 'approved'
  GROUP BY 1,2,3,4
  ) data
LEFT JOIN shared_tetris.publishers p on p.id = data.publisher_id
GROUP BY 1,2,3,4