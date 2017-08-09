select  date ,
sites.name site_name,
install_publisher_id,
publishers.name as install_publisher_name,
daily_distinct_users,
daily_distinct_paying_users,
daily_revenue_sum
from (
SELECT
            'D' lvl,
      date(log_created) date,
      site_id,
      install_publisher_id,
      install_ad_network_id,
      count(distinct(install_id_unique1)) daily_distinct_users ,
      count(distinct(if(revenues_usd > 0,install_id_unique1,null))) daily_distinct_paying_users ,
      sum(revenues_usd) daily_revenue_sum
      FROM `log_conversions`
      WHERE
      log_type  IN ('install', 'open', 'event') AND
      debug_mode = 0 AND test_profile_id = 0 AND
      log_status = 'approved'
        and log_created >=  '2015-06-01'
      and log_created < date_add('2015-06-30', interval 1 day)
                        and advertiser_id = 3444
      group by 1,2,3,4,5
) data
LEFT JOIN (
    SELECT
      id,  name
FROM shared_tetris.sites
    WHERE
      advertiser_id = 3444
  ) sites ON
    sites.id = data.site_id
LEFT JOIN (
    SELECT
      id, name
    FROM shared_tetris.publishers
  ) publishers ON
    publishers.id = data.install_publisher_id