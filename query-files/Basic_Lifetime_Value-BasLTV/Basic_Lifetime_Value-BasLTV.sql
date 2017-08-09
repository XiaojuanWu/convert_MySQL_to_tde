SELECT
  3444 advertiser_id,
  site_id,
  sites.name as site_name,
  package_name,
  publisher_id,
  ifnull(publishers.name, 'Organic') as publisher_name,
  publishers.ad_network_id as ad_network_id,
  date,
  install_interval_day,
  installs,
  events,
  revenues,
  costs
from ( SELECT  site_id, package_name, if(reengagement_publisher_id > 0, reengagement_publisher_id, install_publisher_id) as publisher_id, date(install_created) date,
      install_interval_day,
      SUM(installs) AS installs,
      SUM(events) AS events,
      SUM(revenues_usd) AS revenues,
      SUM(costs) AS costs
    FROM `log_conversions`
    WHERE
      log_type IN ('install', 'open', 'event') AND
      log_status = 'approved'
       and install_created >= '2015-10-01'
       and install_created < date_add('2015-11-30', interval 1 day)
                   and log_created >= '2015-10-01'
                   and log_created < date_add('2015-11-30', interval 31 day)
       and advertiser_id = 3444
    GROUP BY
  1,2,3,4,5
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
    publishers.id = data.publisher_id