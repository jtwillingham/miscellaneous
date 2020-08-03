drop table if exists default.jwillingham_demographic_populations_original;

create external table default.jwillingham_demographic_populations_original
  (
  `demo_name` string,
  `level_name` string,
  `level_code` int,
  `population` int
  )
row format delimited fields terminated by '|'
location 's3://dxinnovation-useast1-dev/tv/panel_development/data/demographic_populations/' tblproperties ('skip.header.line.count'='1');

drop table if exists default.jwillingham_demographic_populations;

create table default.jwillingham_demographic_populations as
select demo_name,
       level_name,
       level_code,
       sum(population) population
from (select demo_name,
             case
               when demo_name = 'hh_age' and level_code in (1,2) then '15 - 34'
               when demo_name = 'hh_age' and level_code in (3,4) then '35 - 54'
               when demo_name = 'hh_age' and level_code in (5,6,7,8,9) then '55 +'
               when demo_name = 'hh_income' and level_code in (1,2,3) then '< $20,000'
               when demo_name = 'hh_income' and level_code in (4,5,6,7,8,9) then '$20,000 - $49,999'
               when demo_name = 'hh_income' and level_code in (10,11,12) then '$50,000 - $99,999'
               when demo_name = 'hh_income' and level_code in (13,14,15,16) then '$100,000 +'
               when demo_name = 'hh_size' and level_code in (1,2) then '1 - 2'
               when demo_name = 'hh_size' and level_code in (3,4) then '3 - 4'
               when demo_name = 'hh_size' and level_code in (5,6,7) then '5 +'
               when demo_name = 'population_density' and level_code in (6,8) then 'Urban population less than 20,000, adjacent to a metro area'
               when demo_name = 'population_density' and level_code in (7,9) then 'Urban population less than 20,000, not adjacent to a metro area'
               else level_name
             end level_name,
             case
               when demo_name = 'hh_age' and level_code in (1,2) then 1
               when demo_name = 'hh_age' and level_code in (3,4) then 2
               when demo_name = 'hh_age' and level_code in (5,6,7,8,9) then 3
               when demo_name = 'hh_income' and level_code in (1,2,3) then 1
               when demo_name = 'hh_income' and level_code in (4,5,6,7,8,9) then 2
               when demo_name = 'hh_income' and level_code in (10,11,12) then 3
               when demo_name = 'hh_income' and level_code in (13,14,15,16) then 4
               when demo_name = 'hh_size' and level_code in (1,2) then 1
               when demo_name = 'hh_size' and level_code in (3,4) then 2
               when demo_name = 'hh_size' and level_code in (5,6,7) then 3
               when demo_name = 'population_density' and level_code in (6,8) then 6
               when demo_name = 'population_density' and level_code in (7,9) then 7
               else level_code
             end level_code,
             population
      from default.jwillingham_demographic_populations_original) tb
group by demo_name,
         level_name,
         level_code;