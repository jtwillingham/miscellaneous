-- DATAXU-MATCHED INSCAPE HOUSEHOLDS WITH VIEWERSHIP DATA
drop table if exists default.jwillingham_linear_households;

create table default.jwillingham_linear_households as
select distinct hhid,
       dma_code,
       broadcast_date,
       'commercial' viewership_type
from default.jwillingham_linear_commercials
union
select distinct hhid,
       dma_code,
       broadcast_date,
       'program' viewership_type
from default.jwillingham_linear_content;

-- CREATE UNWEIGHTED LINEAR PANEL W/O CHURN CONTROL
-- Daily (2018-02-01 thru 2018-02-28) AND 2018 Broadcast Weeks 6, 7 and 8
drop table if exists default.jwillingham_linear_panel_unweighted;

create table default.jwillingham_linear_panel_unweighted as
select distinct hhid,
       dma_code,
       0 time_period,
       broadcast_date intab_start_date,
       viewership_type
from default.jwillingham_linear_households
union all
select distinct hhid,
       dma_code,
       1 time_period,
       cast('2018-02-05' as date) intab_start_date,
       viewership_type
from default.jwillingham_linear_households
where broadcast_date >= '2018-02-05'
and   broadcast_date <= '2018-02-11'
union all
select distinct hhid,
       dma_code,
       1 time_period,
       cast('2018-02-12' as date) intab_start_date,
       viewership_type
from default.jwillingham_linear_households
where broadcast_date >= '2018-02-12'
and   broadcast_date <= '2018-02-18'
union all
select distinct hhid,
       dma_code,
       1 time_period,
       cast('2018-02-19' as date) intab_start_date,
       viewership_type
from default.jwillingham_linear_households
where broadcast_date >= '2018-02-19'
and   broadcast_date <= '2018-02-25';