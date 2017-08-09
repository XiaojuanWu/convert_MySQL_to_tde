SELECT
  3444 advertiser_id,
  site_id,
  sites.name as site_name,
  install_publisher_id,
  publishers.name as install_publisher_name,
  publishers.ad_network_id as install_ad_network_id,
  date,
  cohort_interval_day,
  installs1,
  installs,
  revenues_total,
  events_total,
  revenues_cum,
  events_cum,
  revenues,
  events
 FROM (
  SELECT
    site_id,
    install_publisher_id,
    date,
    cohort_interval_day,
    installs as installs1,
    SUM(installs) OVER (PARTITION BY  site_id,install_publisher_id, date) AS installs,
    SUM(revenues) OVER (PARTITION BY  site_id,install_publisher_id, date) AS revenues_total,
    SUM(events) OVER (PARTITION BY  site_id,install_publisher_id, date) AS events_total,
    SUM(revenues) OVER (PARTITION BY  site_id,install_publisher_id, date ORDER BY cohort_interval_day ROWS UNBOUNDED PRECEDING) AS revenues_cum,
    SUM(events) OVER (PARTITION BY  site_id,install_publisher_id, date ORDER BY cohort_interval_day ROWS UNBOUNDED PRECEDING) AS events_cum,
    revenues,
    events
  FROM (
    SELECT
    users.site_id,
    users.install_publisher_id,
    DATE(users.log_created) AS date,
    logs.install_interval_day as cohort_interval_day,
    SUM(logs.installs) AS installs,
    SUM(logs.revenues_usd) AS revenues,
    SUM(logs.events) AS events
    FROM `log_conversions` logs
    JOIN (
    SELECT
      install_id_unique1,
      site_id,
      install_publisher_id,
      log_created
    FROM `log_conversions`
    WHERE
      debug_mode = 0 AND test_profile_id = 0 AND
      log_status = 'approved'
      and log_created >=  '2015-06-01'
      and log_created < date_add('2015-06-30', interval 1 day)
                  and log_type = 'install'
      and advertiser_id = 3444
    ) users  ON
    users.install_id_unique1 = logs.install_id_unique1
    WHERE
    logs.debug_mode = 0 AND logs.test_profile_id = 0 AND
    logs.log_status = 'approved'
    and logs.log_created >=  '2015-06-01'
    and logs.log_created < date_add('2015-06-30', interval 30 day)
    and logs.advertiser_id = 3444
    GROUP BY
    site_id,
    install_publisher_id,
    date,
    cohort_interval_day
    ) summary
) data
LEFT JOIN (
    SELECT
      id,
    name,
    mobile_app_type,
    status
    FROM shared_tetris.sites
    WHERE
      advertiser_id = 3444
  ) sites ON
    sites.id = data.site_id
LEFT JOIN (
    SELECT
      id,
    name,
    ad_network_id,
    status
    FROM shared_tetris.publishers
   ) publishers ON
    publishers.id = data.install_publisher_id