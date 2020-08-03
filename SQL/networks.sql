drop table if exists default.jwillingham_networks_original;

create external table default.jwillingham_networks_original
  (
  `station_call_sign` string,
  `station_name` string,
  `dma_name` string,
  `station_affiliate` string,
  `station_time_zone` string,
  `national_network_flag` int,
  `network_name` string
  )
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/networks/' tblproperties ('skip.header.line.count'='1');