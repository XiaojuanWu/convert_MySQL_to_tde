SELECT
    3444 advertiser_id,
    sites.name as site_name,
    event_type,
    site_events.name as site_event_name,
    ifnull(publishers.name,'Organic') as publisher_name,
/* Use:  BasEngSubPub for sub_publisher and sub_campaign  */
/* Use:  BasEngGeo    for country_name and region_name    */
    date,
        open_count,
    event_count,
        install_count
FROM ( SELECT   site_id,
                event_type,
                site_event_id,
        if(reengagement_publisher_id > 0, reengagement_publisher_id, install_publisher_id) as publisher_id,
        DATE(log_created) AS date,
        sum(opens) open_count,
        sum(if(log_type = 'event',1,0)) as event_count,
                sum(installs) AS install_count
    FROM log_conversions
    WHERE   debug_mode = 0 and test_profile_id = 0 and log_status = 'approved'
       and log_created >=  '2015-10-01'
       and log_created < date_add('2015-11-30', interval 1 day)
           and advertiser_id = 3444
    GROUP BY 1,2,3,4,5
        ) data
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.sites
    WHERE advertiser_id = 3444
  ) sites ON
    sites.id = data.site_id
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.publishers
  ) publishers ON
    publishers.id = data.publisher_id
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.site_events
    WHERE advertiser_id = 3444
  ) site_events ON
    site_events.id = data.site_event_id