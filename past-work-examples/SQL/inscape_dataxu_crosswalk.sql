drop table if exists default.jwillingham_inscape_dataxu_crosswalk_step1;

create table default.jwillingham_inscape_dataxu_crosswalk_step1 as
select tb1.*,
       rand(21347) random_number
from default.tv_inscape_ip_to_tv_with_hh_consolidated_singlehh tb1
  join (select tvid,
               ipaddress,
               max(lastseen) lastseen_max
        from default.tv_inscape_ip_to_tv_with_hh_consolidated_singlehh
        group by tvid,
                 ipaddress) tb2
    on tb1.tvid = tb2.tvid
   and tb1.ipaddress = tb2.ipaddress
   and tb1.lastseen = tb2.lastseen_max;

drop table if exists default.jwillingham_inscape_dataxu_crosswalk_step2;

create table default.jwillingham_inscape_dataxu_crosswalk_step2 as
select tvid,
       ipaddress,
       max(random_number) random_number_max
from default.jwillingham_inscape_dataxu_crosswalk_step1
group by tvid,
         ipaddress;

drop table if exists default.jwillingham_inscape_dataxu_crosswalk_step3;

create table default.jwillingham_inscape_dataxu_crosswalk_step3 as
select tb1.*
from default.jwillingham_inscape_dataxu_crosswalk_step1 tb1
  join default.jwillingham_inscape_dataxu_crosswalk_step2 tb2
    on tb1.tvid = tb2.tvid
   and tb1.ipaddress = tb2.ipaddress
   and tb1.random_number = tb2.random_number_max;