SELECT
   3444 advertiser_id,
         sites.name site_name,
         date,
         period,
         revenue,
     events,
     publishers.name
from (
     SELECT
  site_id,
  install_publisher_id,
  date(install_created) date,
    CEILING(IF(DATEDIFF(log_created,install_created) <= 0, 1, DATEDIFF(log_created,install_created)) ) AS period,
    sum(revenues_usd) revenue,
  sum(events) events
  FROM `log_conversions`
  WHERE debug_mode = 0 AND test_profile_id = 0 AND
  log_status = 'approved'
  and advertiser_id = 3444
     and install_created >= '2016-01-01'
     and install_created < date_add('2016-01-31', interval 1 day)
           and log_created between '2016-01-01' and date_add('2016-01-31', interval 30 day)
     group by 1,2,3,4
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
Left JOIN shared_tetris.publishers
  ON data.install_publisher_id = publishers.id