drop table if exists default.jwillingham_dayparts_original;

create external table default.jwillingham_dayparts_original
  (
  `half_hour` string,
  `time_zone_group` string,
  `daypart` string
  )
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/dayparts/' tblproperties ('skip.header.line.count'='1');