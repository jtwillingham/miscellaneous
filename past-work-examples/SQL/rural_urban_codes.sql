drop table if exists default.jwillingham_rural_urban_codes;

create external table default.jwillingham_rural_urban_codes
(
  `fips_county` string,
  `state_abbreviation` string,
  `county_name` string,
  `population_2010` int,
  `rural_urban_county_code_2013` int,
  `description` string
)
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/rural_urban_codes/' tblproperties ('skip.header.line.count'='1');