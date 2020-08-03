drop table if exists default.jwillingham_linear_commercials_original;

create external table default.jwillingham_linear_commercials_original
  (
  `tvid` string,
  `zipcode` string,
  `dma` string,
  `commercial_id` string,
  `commercial_start_media_time` int,
  `commercial_start` timestamp,
  `commercial_end` timestamp,
  `previous_content_tms_id` string,
  `previous_content_title` string,
  `previous_content_start` timestamp,
  `previous_content_end` timestamp,
  `previous_network_callsign` string,
  `next_content_tms_id` string,
  `next_content_title` string,
  `next_content_start` timestamp,
  `next_content_end` timestamp,
  `next_network_callsign` string,
  `commercial_brand` string,
  `commercial_title` string,
  `commercial_length` int,
  `ipaddress` string
  )
partitioned by (`filedate` date)
row format delimited fields terminated by ','
location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/';

alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-01') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-01/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-02') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-02/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-03') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-03/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-04') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-04/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-05') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-05/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-06') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-06/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-07') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-07/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-08') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-08/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-09') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-09/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-10') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-10/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-11') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-11/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-12') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-12/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-13') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-13/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-14') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-14/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-15') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-15/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-16') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-16/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-17') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-17/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-18') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-18/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-19') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-19/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-20') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-20/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-21') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-21/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-22') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-22/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-23') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-23/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-24') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-24/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-25') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-25/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-26') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-26/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-27') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-27/';
alter table default.jwillingham_linear_commercials_original add partition (filedate='2018-02-28') location 's3://dxinnovation-useast1-dev/tv/acr/commercialmetadataip/2018-02-28/';

drop table if exists default.jwillingham_linear_commercials;

create table default.jwillingham_linear_commercials as
select distinct hhid,
       tvid,
       zipcode,
       city,
       state,
       dma_code,
       time_zone,
       time_zone_group,
       commercial_id,
       commercial_start_media_time,
       commercial_start_utc,
       commercial_end_utc,
       commercial_start_normalized,
       commercial_end_normalized,
       commercial_brand,
       commercial_title,
       commercial_length,
       commercial_recognition_duration,
       previous_content_tms_id,
       previous_content_title,
       previous_content_start_utc,
       previous_content_end_utc,
       previous_content_start_normalized,
       previous_content_end_normalized,
       previous_station_call_sign,
       previous_network_name,
       previous_national_network_flag,
       next_content_tms_id,
       next_content_title,
       next_content_start_utc,
       next_content_end_utc,
       next_content_start_normalized,
       next_content_end_normalized,
       next_station_call_sign,
       next_network_name,
       next_national_network_flag,
       cast(date_format(commercial_start_normalized,'yyyy-MM-dd') as date) broadcast_date,
       date_format(from_unixtime(unix_timestamp(commercial_start_normalized) - (unix_timestamp(commercial_start_normalized) % 1800)),'HH:mm') half_hour,
       date_format(commercial_start_normalized,'u') dayofweek,
       case
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) between 2 and 5 then 'Overnight'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) between 6 and 8 then 'Early Morning'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) between 9 and 15 then 'Daytime'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) between 16 and 18 then 'Early Fringe'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) = 19 then 'Prime Access'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) between 20 and 22 then 'Primetime'
         when time_zone_group = 'Eastern/Pacific' and hour(commercial_start_normalized) in (23,0,1) then 'Late Fringe'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) between 1 and 4 then 'Overnight'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) between 5 and 7 then 'Early Morning'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) between 8 and 14 then 'Daytime'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) between 15 and 17 then 'Early Fringe'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) = 18 then 'Prime Access'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) between 19 and 21 then 'Primetime'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(commercial_start_normalized) in (22,23,0) then 'Late Fringe'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) in (23,0,1,2) then 'Overnight'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) between 3 and 5 then 'Early Morning'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) between 6 and 12 then 'Daytime'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) between 13 and 15 then 'Early Fringe'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) = 16 then 'Prime Access'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) between 17 and 19 then 'Primetime'
         when time_zone_group = 'Hawaiian' and hour(commercial_start_normalized) between 20 and 22 then 'Late Fringe'
         else 'unknown'
       end daypart,
       ipaddress,
       filedate
from (select tb1.*,
             tb2.hhid,
             tb2.state,
             tb2.dma_code,
             tb4.station_call_sign previous_station_call_sign,
             tb4.network_name previous_network_name,
             tb4.national_network_flag previous_national_network_flag,
             tb5.station_call_sign next_station_call_sign,
             tb5.network_name next_network_name,
             tb5.national_network_flag next_national_network_flag,
             case
               when tb2.zipcode != '' then tb2.zipcode
               else 'unknown'
             end zipcode,
             case
               when tb2.city != 'null' then tb2.city
               else 'unknown'
             end city,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then 'America/New_York'
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then 'America/Chicago'
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then 'America/Denver'
               when tb2.state in ('AZ') then 'America/Phoenix'
               when tb2.state in ('CA','NV','OR','WA') then 'America/Los_Angeles'
               when tb2.state in ('AK') then 'America/Anchorage'
               when tb2.state in ('HI') then 'Pacific/Honolulu'
               else 'unknown'
             end time_zone,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then 'Eastern/Pacific'
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then 'Central/Mountain/Alaskan'
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then 'Central/Mountain/Alaskan'
               when tb2.state in ('AZ') then 'Central/Mountain/Alaskan'
               when tb2.state in ('CA','NV','OR','WA') then 'Eastern/Pacific'
               when tb2.state in ('AK') then 'Central/Mountain/Alaskan'
               when tb2.state in ('HI') then 'Hawaiian'
               else 'unknown'
             end time_zone_group,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.commercial_start_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.commercial_start_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.commercial_start_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.commercial_start_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.commercial_start_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.commercial_start_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.commercial_start_utc,'Pacific/Honolulu')
               else tb1.commercial_start_utc
             end commercial_start_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.commercial_end_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.commercial_end_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.commercial_end_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.commercial_end_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.commercial_end_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.commercial_end_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.commercial_end_utc,'Pacific/Honolulu')
               else tb1.commercial_end_utc
             end commercial_end_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.previous_content_start_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.previous_content_start_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.previous_content_start_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.previous_content_start_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.previous_content_start_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.previous_content_start_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.previous_content_start_utc,'Pacific/Honolulu')
               else tb1.previous_content_start_utc
             end previous_content_start_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.previous_content_end_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.previous_content_end_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.previous_content_end_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.previous_content_end_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.previous_content_end_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.previous_content_end_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.previous_content_end_utc,'Pacific/Honolulu')
               else tb1.previous_content_end_utc
             end previous_content_end_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.next_content_start_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.next_content_start_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.next_content_start_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.next_content_start_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.next_content_start_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.next_content_start_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.next_content_start_utc,'Pacific/Honolulu')
               else tb1.next_content_start_utc
             end next_content_start_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.next_content_end_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.next_content_end_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.next_content_end_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.next_content_end_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.next_content_end_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.next_content_end_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.next_content_end_utc,'Pacific/Honolulu')
               else tb1.next_content_end_utc
             end next_content_end_normalized
      from (select tvid,
                   commercial_id,
                   commercial_start_media_time,
                   commercial_start commercial_start_utc,
                   commercial_end commercial_end_utc,
                   commercial_brand,
                   commercial_title,
                   case
                     when commercial_length % 15 = 0 then commercial_length
                     when commercial_length % 15 > 7 then 15 + commercial_length - commercial_length % 15
                     else commercial_length - commercial_length % 15
                   end commercial_length,
                   unix_timestamp(commercial_end) - unix_timestamp(commercial_start) commercial_recognition_duration,
                   previous_content_tms_id,
                   previous_content_title,
                   previous_content_start previous_content_start_utc,
                   previous_content_end previous_content_end_utc,
                   previous_network_callsign,
                   next_content_tms_id,
                   next_content_title,
                   next_content_start next_content_start_utc,
                   next_content_end next_content_end_utc,
                   next_network_callsign,
                   ipaddress,
                   filedate
            from default.jwillingham_linear_commercials_original
            where substr(previous_content_tms_id,1,2) in ('EP','MV','SH','SP')) tb1
            join (select tb1.tvid,
                         tb1.hhid,
                         tb1.zipcode,
                         tb1.city,
                         tb1.state,
                         tb2.nielsen_dma_code dma_code
                  from default.jwillingham_inscape_dataxu_crosswalk_step3 tb1
                    join default.jwillingham_dma_xwalk tb2 on tb1.dma = tb2.inscape_dma_name) tb2 on tb1.tvid = tb2.tvid
            join default.jwillingham_networks_original tb4 on tb1.previous_network_callsign = tb4.station_call_sign
            join default.jwillingham_networks_original tb5 on tb1.next_network_callsign = tb5.station_call_sign) tb0;