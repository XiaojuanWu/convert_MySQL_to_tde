SELECT
    advertiser_id,
    sites.name as site_name,
    publishers.name as publisher_name,
    publishers_attributed.name as attributed_publisher_name,
    date,
    is_nonwindowed_attribution,
    is_assist,
    installs
FROM (
    SELECT
        la.advertiser_id,
        la.site_id,
        la.publisher_id,
        la.attributed_publisher_id,
        DATE(la.conversion_created) AS date,
        la.is_nonwindowed_attribution,
        IF( la.attributed_publisher_id > 0, IF(la.publisher_id != la.attributed_publisher_id , 1,0),0) as is_assist,
        count(*) AS installs
     FROM   log_attributions la
                    join log_conversions lc
                              ON lc.log_id = la.conversion_id
                WHERE  1 = 1
                       AND la.conversion_type = 'install'
                       AND la.conversion_created >= Date('2015-10-01')
                       AND la.conversion_created < Date_add(Date('2015-11-30'),
                                                   INTERVAL 1 day)
                        and lc.is_reengagement = 0
                                          AND lc.log_status = 'approved'
                                          AND lc.debug_mode = 0
                                          AND 1 = 1
                                          AND lc.log_created >= Date('2015-10-01')
                                          AND lc.log_created < Date_add(Date('2015-11-30'), INTERVAL 1 day )
    GROUP BY
        la.advertiser_id,
        la.site_id,
        la.publisher_id,
        la.attributed_publisher_id,
        date,
        is_nonwindowed_attribution,
        is_assist
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
    publishers.id = data.publisher_id
LEFT JOIN (
    SELECT
      id,
      name
    FROM shared_tetris.publishers
  ) publishers_attributed ON
    publishers_attributed.id = data.attributed_publisher_id