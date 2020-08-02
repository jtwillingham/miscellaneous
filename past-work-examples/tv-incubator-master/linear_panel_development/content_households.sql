-- DATAXU-MATCHED INSCAPE HOUSEHOLDS WITH PROGRAM DATA
-- https://api.qubole.com/v2/analyze?command_id=149934974
set hive.llap.execution.mode = auto;

drop table if exists default.jwillingham_inscape_program_households_step1;

create table default.jwillingham_inscape_program_households_step1 as
select distinct hhid
from default.jwillingham_linear_content_original tb1
  join default.tv_inscape_ip_to_tv_with_hh_consolidated tb2 on tb1.tvid = tb2.tvid;

-- OBTAIN GRAPH IDS
-- https://api.qubole.com/v2/analyze?command_id=150283188
set hive.llap.execution.mode = auto;

drop table if exists default.jwillingham_inscape_program_households_step2;

create table default.jwillingham_inscape_program_households_step2 as
select distinct id
from (select clusterid
      from default.jwillingham_inscape_program_households_step1 tb1
        join default.tm_multihop_graph_part_old tb2 on tb1.hhid = tb2.id) tb1
  join default.tm_multihop_graph_part_old tb2 on tb1.clusterid = tb2.clusterid;

-- OBTAIN ACXIOM HOUSEHOLDS
-- https://api.qubole.com/v2/analyze?command_id=150462119
set hive.llap.execution.mode = auto;

drop table if exists default.jwillingham_inscape_program_households_step3;

create table default.jwillingham_inscape_program_households_step3 as
select distinct household_pel
from default.jwillingham_inscape_program_households_step2 tb1
  join (select alias_id,
               alias_type,
               individual_link
        from default.acxiom_identitylink_orc) tb2 on tb1.id = concat(tb2.alias_id,'_',tb2.alias_type)
  join default.jwillingham_acxiom_paf tb3 on tb2.individual_link = tb3.consumer_pel
where household_pel != '';