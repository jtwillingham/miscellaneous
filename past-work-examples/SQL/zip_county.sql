drop table if exists default.jwillingham_zip_county_original;

create external table default.jwillingham_zip_county_original
(
  `zip_code` string,
  `fips_county` string,
  `res_ratio` double,
  `bus_ratio` double,
  `oth_ratio` double,
  `tot_ratio` double
)
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/zip_county/' tblproperties ('skip.header.line.count'='1');

drop table if exists default.jwillingham_zip_county;

create table default.jwillingham_zip_county as
select tbl.zip_code,
       tbl.fips_county,
       tbl.res_ratio,
       tbl.bus_ratio,
       tbl.oth_ratio,
       tbl.tot_ratio
from (select *,
             row_number() over (partition by zip_code order by res_ratio desc, bus_ratio desc, oth_ratio desc, tot_ratio desc) rn
      from default.jwillingham_zip_county_original) tbl
where tbl.rn = 1;