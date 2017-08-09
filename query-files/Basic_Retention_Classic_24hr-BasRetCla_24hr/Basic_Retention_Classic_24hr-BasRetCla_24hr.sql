SELECT
   3444 advertiser_id,
         site_id,
         sites.name site_name,
         install_publisher_id,
         publishers.name publisher_name,
         date,
         period,
         users retained_users
from (
      SELECT
  site_id,
  package_name,
  install_publisher_id,
  date(install_created) date,
/* -- Next "Day" defined as 24 hours subsequent to install.  Example Mon 11 am will be same Day on Tuesday before 11 am, next Day 11 am or after  --- */
  CEILING(IF(TIMESTAMPDIFF(HOUR, install_created, log_created) <= 0, 1, TIMESTAMPDIFF(HOUR, install_created, log_created)) / 24) AS period,
  count(distinct(install_id_unique1)) users
  FROM `log_conversions`
  WHERE
  log_type IN ('install','open') AND
  debug_mode = 0 AND test_profile_id = 0 AND
  log_status = 'approved'
     and log_created >=  '2015-06-01'
     and log_created < date_add('2015-06-30', interval 30 day)
     and install_created >=  '2015-06-01'
     and install_created < date_add('2015-06-30', interval 1 day)
  and advertiser_id = 3444
     group by 1,2,3,4,5
) data
LEFT JOIN (
    SELECT
      id,
    name
  FROM shared_tetris.sites
    WHERE
      advertiser_id = 3444
  ) sites ON
    sites.id = data.site_id
LEFT JOIN (
    SELECT
      id,
    name
    FROM shared_tetris.publishers
  ) publishers ON
    publishers.id = data.install_publisher_id