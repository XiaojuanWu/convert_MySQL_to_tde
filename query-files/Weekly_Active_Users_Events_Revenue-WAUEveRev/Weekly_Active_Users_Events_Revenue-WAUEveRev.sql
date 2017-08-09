SELECT week,
p.name as publisher,
device_type,
events,
weekly_users,
revenues_usd
FROM (
  SELECT date(subdate(log_created,weekday(log_created)+1)) as week,
  install_publisher_id,
  device_type,
  site_id,
  site_event_id,
    revenues_usd,
  sum(events) as events,
  sum(opens) as opens,
  count(distinct install_id_unique1) as weekly_users
  FROM log_conversions
  WHERE advertiser_id = 3444
  and log_type in ('open','event')
  and log_created >= '2015-10-01'
  and log_created < date_add('2015-11-30', interval 1 day)
  and debug_mode = 0 and test_profile_id = 0 and log_status = 'approved'
  GROUP BY 1,2,3,4,5,6) d
LEFT JOIN shared_tetris.publishers p on p.id = d.install_publisher_id
LEFT JOIN (
  SELECT id, name
  FROM shared_tetris.sites
  WHERE advertiser_id = 3444
  and status = 'active') s on s.id = d.site_id
LEFT JOIN (
  SELECT id, name
  FROM shared_tetris.site_events
  WHERE advertiser_id = 3444) se on se.id = d.site_event_id