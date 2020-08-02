-- OBTAIN CTV TRANSACTIONS
-- https://api.qubole.com/v2/analyze?command_id=139842818
set hive.llap.execution.mode = auto;
set startDate = 2018-01-01;
set endDate = 2018-03-25;

drop table if exists default.jwillingham_ctv_transactions_part1;

create table default.jwillingham_ctv_transactions_part1 as
select flight_id,
       transaction_date,
       alias_id_type,
       case
         when impression_flag = 1 then 1
         else 0
       end impression_flag
from (select flight_id,
             cast(concat(substr(cast(t as string),5,4),'-',substr(cast(t as string),3,2),'-',substr(cast(t as string),1,2)) as date) transaction_date,
             concat(alias_info['alias_id'],'_',alias_info['alias_type']) alias_id_type
      from default.tv_ctv_transactions_history_log lateral view explode (incoming_aliases) alias_table as alias_info
      where cast(concat(substr(cast(t as string),5,4),'-',substr(cast(t as string),3,2),'-',substr(cast(t as string),1,2)) as date) >= '${hiveconf:startDate}'
      and   cast(concat(substr(cast(t as string),5,4),'-',substr(cast(t as string),3,2),'-',substr(cast(t as string),1,2)) as date) <= '${hiveconf:endDate}') tb1
  left join (select flight_uid,
                    cast(pday as date) impression_date,
                    concat(alias_info['alias_id'],'_',alias_info['alias_type']) id,
                    1 impression_flag
             from default.impressions lateral view explode (incoming_aliases) alias_table as alias_info
             where pday between '${hiveconf:startDate}' and '${hiveconf:endDate}') tb2
         on tb1.flight_id = tb2.flight_uid
        and tb1.transaction_date = tb2.impression_date
        and tb1.alias_id_type = tb2.id;

-- OBTAIN GRAPH COMPONENTS
-- https://api.qubole.com/v2/analyze?command_id=139986178
set hive.llap.execution.mode=auto;
set startDate = 17022018;
set endDate = 16032018;
set graphType = lt460_ids;

drop table if exists default.jwillingham_ctv_transactions_part2;

create table default.jwillingham_ctv_transactions_part2 as
select distinct flight_id,
       transaction_date,
       component,
       impression_flag
from default.jwillingham_ctv_transactions_part1 tb1
  join default.oy_tv_graph_${hiveconf:startDate}_${hiveconf:endDate}_${hiveconf:graphType} tb2 on tb1.alias_id_type = tb2.id;

-- OBTAIN GRAPH IDS
-- https://api.qubole.com/v2/analyze?command_id=140015424
set hive.llap.execution.mode=auto;
set startDate = 17022018;
set endDate = 16032018;
set graphType = lt460_ids;

drop table if exists default.jwillingham_ctv_transactions_part3;

create table default.jwillingham_ctv_transactions_part3 as
select distinct flight_id,
       transaction_date,
       id,
       impression_flag
from default.jwillingham_ctv_transactions_part2 tb1
  join default.oy_tv_graph_${hiveconf:startDate}_${hiveconf:endDate}_${hiveconf:graphType} tb2 on tb1.component = tb2.component;

-- MATCH CTV TRANSACTIONS TO ACXIOM CONSUMERS
-- https://api.qubole.com/v2/analyze?command_id=140250207
set hive.llap.execution.mode=auto;
set identitylinkFilter = 20171201;
set startDate = 2018-01-01;
set endDate = 2018-03-25;

drop table if exists default.jwillingham_ctv_transactions_part4;

create table default.jwillingham_ctv_transactions_part4 as
select distinct flight_id,
       transaction_date,
       consumer_pel,
       impression_flag
from default.jwillingham_ctv_transactions_part3 tb1
  join (select alias_id,
               alias_type,
               individual_link
        from default.acxiom_identitylink_orc
        where cast(delivery_date as int) > ${hiveconf:identitylinkFilter}) tb2 on tb1.id = concat(tb2.alias_id,'_',tb2.alias_type)
  join default.jwillingham_acxiom_paf tb3 on tb2.individual_link = tb3.consumer_pel
where transaction_date >= '${hiveconf:startDate}'
and   transaction_date <= '${hiveconf:endDate}';

-- OBTAIN ACXIOM HOUSEHOLDS
-- https://api.qubole.com/v2/analyze?command_id=140252261
set hive.llap.execution.mode = auto;

drop table if exists default.jwillingham_ctv_transactions_part5;

create table default.jwillingham_ctv_transactions_part5 as
select distinct flight_id,
       transaction_date,
       household_pel,
       impression_flag
from default.jwillingham_ctv_transactions_part4 tb1
  join default.jwillingham_acxiom_paf tb2 on tb1.consumer_pel = tb2.consumer_pel
where household_pel != '';

-- RENAME TABLES
-- https://api.qubole.com/v2/analyze?command_id=140279583
alter table default.jwillingham_ctv_transactions_part1 rename to default.jwillingham_ctv_transactions_step1;
alter table default.jwillingham_ctv_transactions_part2 rename to default.jwillingham_ctv_transactions_step2;
alter table default.jwillingham_ctv_transactions_part3 rename to default.jwillingham_ctv_transactions_step3;
alter table default.jwillingham_ctv_transactions_part4 rename to default.jwillingham_ctv_transactions_step4;
alter table default.jwillingham_ctv_transactions_part5 rename to default.jwillingham_ctv_transactions_step5;