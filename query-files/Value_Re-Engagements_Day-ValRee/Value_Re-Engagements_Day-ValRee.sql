SELECT
  advertiser_id,
  site_id,
  sites.name as site_name,
  package_name,
  reengagement_publisher_id,
  publishers.name as reengagement_publisher_name,
  reengagement_ad_network_id,
  date,
  cohort_interval_day,
  reengagements,
  revenues_total,
  view_through_reengagements,
  click_through_reengagements,
  revenues,
  revenues_cum
FROM (
  SELECT
    advertiser_id,
    site_id,
    package_name,
    reengagement_publisher_id,
    reengagement_ad_network_id,
    date,
    cohort_interval_day,
    SUM(reengagements) OVER (PARTITION BY site_id, reengagement_publisher_id, date) AS reengagements,
    SUM(revenues) OVER (PARTITION BY site_id, reengagement_publisher_id, date) AS revenues_total,
    view_through_reengagements,
    click_through_reengagements,
    revenues,
    SUM(revenues) OVER (PARTITION BY site_id, reengagement_publisher_id, date ORDER BY cohort_interval_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS revenues_cum
  FROM (
    SELECT
    users.advertiser_id,
    users.site_id,
    users.package_name,
    users.reengagement_publisher_id,
    users.reengagement_ad_network_id,
    DATE(users.log_created) AS date,
    IFNULL(logs.reengagement_interval_day, 0) AS cohort_interval_day,
    SUM(IFNULL(logs.view_through_reengagements, 0)) AS view_through_reengagements,
    SUM(IFNULL(logs.click_through_reengagements, 0)) AS click_through_reengagements,
    SUM(IFNULL(logs.reengagements, 0)) AS reengagements,
    SUM(IFNULL(logs.revenues, 0)) AS revenues
    FROM  (
    SELECT
      log_id AS reengagement_id,
      log_type,
      advertiser_id,
      site_id,
      package_name,
      reengagement_publisher_id,
      reengagement_ad_network_id ,
      log_created
    FROM `log_conversions`
    WHERE
      log_type IN ('open', 'event') AND
      is_reengagement = 1 AND
      debug_mode = 0 AND test_profile_id = 0 AND
      log_status = 'approved'
     and log_created >=  '2015-10-01'
     and log_created < date_add('2015-11-30', interval 1 day)
           and is_reengagement = 1
     and advertiser_id = 3444
    ) users
    LEFT JOIN (
    SELECT
      reengagement_id,
      reengagement_interval_day,
      SUM(if(is_reengagement=1, if(reengagement_attributable_type='impression', 1, 0), 0)) AS view_through_reengagements,
      SUM(if(is_reengagement=1, if(reengagement_attributable_type='click', 1, 0), 0)) AS click_through_reengagements,
      SUM(is_reengagement) AS reengagements,
      SUM(revenues_usd) AS revenues
    FROM `log_conversions`
    WHERE
      log_type IN ('open', 'event') AND
      debug_mode = 0 AND test_profile_id = 0 AND
      log_status = 'approved'
     and log_created >=  '2015-10-01'
     and log_created < date_add('2015-11-30', interval 1 day)
     and   (is_reengagement = 1 OR reengagement_id IS NOT NULL)
     and advertiser_id = 3444
    GROUP BY
      reengagement_id,
      reengagement_interval_day
    ) logs ON
    users.reengagement_id = logs.reengagement_id
    GROUP BY
    site_id,
    advertiser_id,
    site_id,
    package_name,
    reengagement_publisher_id,
    reengagement_ad_network_id,
    date,
    cohort_interval_day
  ) summary
  ORDER BY
    site_id,
    advertiser_id,
    site_id,
    package_name,
    reengagement_publisher_id,
    reengagement_ad_network_id ,
    cohort_interval_day
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
    publishers.id = data.reengagement_publisher_id