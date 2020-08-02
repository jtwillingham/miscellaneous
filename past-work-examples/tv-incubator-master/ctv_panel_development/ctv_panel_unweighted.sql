-- CREATE UNWEIGHTED CTV PANEL W/O CHURN CONTROL
-- 2018 Q1 Broadcast Quarter (2018-01-01 through 2018-03-25)
-- https://api.qubole.com/v2/analyze?command_id=140290150
set hive.llap.execution.mode=auto;

drop table if exists default.jwillingham_ctv_panel_unweighted;

create table default.jwillingham_ctv_panel_unweighted as
select tb1.household_pel,
       tb1.time_period,
       tb1.intab_start_date,
       tb2.hh_age_code,
       tb2.hh_income_code,
       tb2.hh_size_code,
       tb2.hh_race_code,
       tb2.population_density_code,
       tb2.dma_code
from (select distinct household_pel,
             0 time_period,
             transaction_date intab_start_date
      from default.jwillingham_ctv_transactions_step5
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-01-01' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-01'
      and   transaction_date <= '2018-01-07'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-01-08' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-08'
      and   transaction_date <= '2018-01-14'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-01-15' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-15'
      and   transaction_date <= '2018-01-21'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-01-22' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-22'
      and   transaction_date <= '2018-01-28'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-01-29' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-29'
      and   transaction_date <= '2018-02-04'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-02-05' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-02-05'
      and   transaction_date <= '2018-02-11'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-02-12' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-02-12'
      and   transaction_date <= '2018-02-18'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-02-19' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-02-19'
      and   transaction_date <= '2018-02-25'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-02-26' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-02-26'
      and   transaction_date <= '2018-03-04'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-03-05' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-03-05'
      and   transaction_date <= '2018-03-11'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-03-12' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-03-12'
      and   transaction_date <= '2018-03-18'
      union all
      select distinct household_pel,
             1 time_period,
             cast('2018-03-19' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-03-19'
      and   transaction_date <= '2018-03-25'
      union all
      select distinct household_pel,
             2 time_period,
             cast('2018-01-01' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-01'
      and   transaction_date <= '2018-01-28'
      union all
      select distinct household_pel,
             2 time_period,
             cast('2018-01-29' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-29'
      and   transaction_date <= '2018-02-25'
      union all
      select distinct household_pel,
             2 time_period,
             cast('2018-02-26' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-02-26'
      and   transaction_date <= '2018-03-25'
      union all
      select distinct household_pel,
             3 time_period,
             cast('2018-01-01' as date) intab_start_date
      from default.jwillingham_ctv_transactions_step5
      where transaction_date >= '2018-01-01'
      and   transaction_date <= '2018-03-25') tb1
  join default.jwillingham_household_demographics_step5 tb2 on tb1.household_pel = tb2.household_pel
where tb2.hh_age_code != 0
and   tb2.hh_income_code != 0
and   tb2.hh_size_code != 0
and   tb2.hh_race_code != 0
and   tb2.population_density_code != 0
and   tb2.dma_code != 0;