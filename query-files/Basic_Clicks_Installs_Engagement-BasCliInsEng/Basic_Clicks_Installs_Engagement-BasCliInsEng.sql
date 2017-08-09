SELECT log_date,
p.name as publisher,
c.name as campaign,
s.name as Site_Name,
sum(clicks) as clicks,
sum(installs) as installs,
sum(opens) as opens,
sum(events) as events
FROM (
    SELECT date(log_created) as log_date,
    publisher_id,
    campaign_id,
        site_id,
    sum(clicks) as clicks,
    0 as installs,
    0 as opens,
    0 as events
    FROM log_attributables
    WHERE advertiser_id = 3444
    AND log_created >= '2015-10-01'
    AND log_created < date_add('2015-11-30', interval 1 day)
    GROUP BY 1,2,3,4
     UNION ALL
    SELECT date(log_created) as log_date,
    install_publisher_id as publisher_id,
    campaign_id,
        site_id,
    0 as clicks,
    sum(installs) as installs,
    sum(opens) as opens,
    sum(events) as events
    FROM log_conversions
    WHERE advertiser_id = 3444
    AND log_created >= '2015-10-01'
    AND log_created < date_add('2015-11-30', interval 1 day)
    AND log_status = 'approved'
    GROUP BY 1,2,3,4) data
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.publishers
    WHERE status = 'active') p ON p.id = publisher_id
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.campaigns
    WHERE advertiser_id = 3444) c ON c.id = campaign_id
LEFT JOIN (
        SELECT id, name
        FROM shared_tetris.sites
        WHERE advertiser_id = 3444) s ON s.id = site_id
GROUP BY 1,2,3,4