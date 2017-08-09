SELECT
  3444 advertiser_id,
    c.name as country,
    data.postal_code,
    p.latitude,
    p.longitude,
  installs,
  events,
  revenues,
  costs,
    opens
from ( SELECT

      country_code,
          region_code,
          postal_code,
      SUM(installs) AS installs,
      SUM(events) AS events,
      SUM(revenues_usd) AS revenues,
      SUM(costs) AS costs,
          SUM(opens) AS opens
    FROM `log_conversions`
    WHERE
      log_type IN ('install', 'open', 'event') AND
      log_status = 'approved'
       and install_created >= '2015-06-01'
       and install_created < date_add('2015-06-30', interval 1 day)
       and advertiser_id = 3444
    GROUP BY
  1,2,3
) data
LEFT JOIN (
    select code, name
    from shared_tetris.countries) c ON
                    c.code = country_code
LEFT JOIN (
  select code, name
  from shared_tetris.regions) r ON
          r.code = region_code
LEFT JOIN (
  select postal_code, country_code, region_code, latitude, longitude
    from shared_tetris.postal_codes) p ON
          p.postal_code = data.postal_code
                    AND
                    p.country_code = data.country_code
                    AND
                    p.region_code = data.region_code