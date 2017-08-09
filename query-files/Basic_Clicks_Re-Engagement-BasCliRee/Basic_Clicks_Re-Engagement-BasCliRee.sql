SELECT
    date(reengagement_created) as Date,
        ifnull(p.name, 'Organic') as Partner,
    ifnull(c.name, 'Organic') as campaign,
    s.name as Apps,
    count(distinct if(reengagement_attributable_type = 'click', reengagement_attributable_id_unique1, null)) as clicks,
    sum(if(reengagement_id = log_id and reengagement_attributable_type = 'click', 1, 0)) as Reengagements
    FROM log_conversions
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.publishers
    WHERE status = 'active') p ON p.id = reengagement_publisher_id
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.campaigns
    WHERE advertiser_id = 3444) c ON c.id = campaign_id
LEFT JOIN (
    SELECT id, name
    FROM shared_tetris.sites
    WHERE advertiser_id = 3444) s ON s.id = site_id
WHERE log_conversions.advertiser_id = 3444
    AND reengagement_publisher_id > 0
    AND log_type in ('open','event')
    AND reengagement_created >= '2015-10-01'
    AND reengagement_created < date_add('2015-11-30', interval 1 day)
    AND log_status = 'approved'
    GROUP BY 1,2,3,4