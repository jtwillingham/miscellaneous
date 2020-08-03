drop table if exists default.jwillingham_linear_content_original;

create external table default.jwillingham_linear_content_original
  (
  `tvid` string,
  `zipcode` string,
  `dma` string,
  `content_tms_id` string,
  `content_title` string,
  `scheduled_content_start_time` string,
  `network_callsign` string,
  `content_start_media_time` int,
  `content_recognition_start` timestamp,
  `content_recognition_end` timestamp,
  `ipaddress` string
  )
partitioned by (`filedate` date)
row format delimited fields terminated by ','
location 's3://dxinnovation-useast1-dev/tv/acr/contentip/';

alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-01') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-01/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-02') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-02/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-03') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-03/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-04') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-04/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-05') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-05/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-06') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-06/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-07') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-07/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-08') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-08/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-09') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-09/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-10') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-10/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-11') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-11/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-12') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-12/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-13') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-13/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-14') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-14/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-15') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-15/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-16') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-16/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-17') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-17/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-18') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-18/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-19') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-19/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-20') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-20/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-21') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-21/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-22') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-22/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-23') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-23/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-24') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-24/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-25') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-25/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-26') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-26/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-27') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-27/';
alter table default.jwillingham_linear_content_original add partition (filedate = '2018-02-28') location 's3://dxinnovation-useast1-dev/tv/acr/contentip/2018-02-28/';

drop table if exists default.jwillingham_linear_content;

create table default.jwillingham_linear_content as
select distinct hhid,
       tvid,
       zipcode,
       city,
       state,
       dma_code,
       time_zone,
       time_zone_group,
       content_tms_id,
       content_title,
       scheduled_content_start_time_utc,
       scheduled_content_start_time_normalized,
       station_call_sign,
       network_name,
       national_network_flag,
       content_start_media_time,
       content_recognition_start_utc,
       content_recognition_end_utc,
       content_recognition_start_normalized,
       content_recognition_end_normalized,
       content_recognition_duration,
       cast(date_format(scheduled_content_start_time_normalized,'yyyy-MM-dd') as date) broadcast_date,
       date_format(from_unixtime(unix_timestamp(scheduled_content_start_time_normalized) - (unix_timestamp(scheduled_content_start_time_normalized) % 1800)),'HH:mm') half_hour,
       date_format(scheduled_content_start_time_normalized,'u') dayofweek,
       live_offset,
       case
         when live_offset < 10800 then 'Live'
         else 'DVR'
       end viewing_source,
       case
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) between 2 and 5 then 'Overnight'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) between 6 and 8 then 'Early Morning'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) between 9 and 15 then 'Daytime'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) between 16 and 18 then 'Early Fringe'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) = 19 then 'Prime Access'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) between 20 and 22 then 'Primetime'
         when time_zone_group = 'Eastern/Pacific' and hour(scheduled_content_start_time_normalized) in (23,0,1) then 'Late Fringe'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) between 1 and 4 then 'Overnight'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) between 5 and 7 then 'Early Morning'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) between 8 and 14 then 'Daytime'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) between 15 and 17 then 'Early Fringe'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) = 18 then 'Prime Access'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) between 19 and 21 then 'Primetime'
         when time_zone_group = 'Central/Mountain/Alaskan' and hour(scheduled_content_start_time_normalized) in (22,23,0) then 'Late Fringe'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) in (23,0,1,2) then 'Overnight'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) between 3 and 5 then 'Early Morning'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) between 6 and 12 then 'Daytime'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) between 13 and 15 then 'Early Fringe'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) = 16 then 'Prime Access'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) between 17 and 19 then 'Primetime'
         when time_zone_group = 'Hawaiian' and hour(scheduled_content_start_time_normalized) between 20 and 22 then 'Late Fringe'
         else 'unknown'
       end daypart,
       ipaddress,
       filedate
from (select tb1.*,
             tb2.hhid,
             tb2.state,
             tb2.dma_code,
             tb3.station_call_sign,
             tb3.network_name,
             tb3.national_network_flag,
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
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.scheduled_content_start_time_utc,'Pacific/Honolulu')
               else tb1.scheduled_content_start_time_utc
             end scheduled_content_start_time_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.content_recognition_start_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.content_recognition_start_utc,'Pacific/Honolulu')
               else tb1.content_recognition_start_utc
             end content_recognition_start_normalized,
             case
               when tb2.state in ('CT','DC','DE','FL','GA','IN','KY','MA','MD','ME','MI','NC','NH','NJ','NY','OH','PA','RI','SC','VA','VT','WV') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/New_York')
               when tb2.state in ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND','NE','OK','SD','TN','TX','WI') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/Chicago')
               when tb2.state in ('CO','ID','MT','NM','UT','WY') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/Denver')
               when tb2.state in ('AZ') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/Phoenix')
               when tb2.state in ('CA','NV','OR','WA') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/Los_Angeles')
               when tb2.state in ('AK') then from_utc_timestamp(tb1.content_recognition_end_utc,'America/Anchorage')
               when tb2.state in ('HI') then from_utc_timestamp(tb1.content_recognition_end_utc,'Pacific/Honolulu')
               else tb1.content_recognition_end_utc
             end content_recognition_end_normalized,
             unix_timestamp(tb1.content_recognition_start_utc) - unix_timestamp(tb1.scheduled_content_start_time_utc) live_offset
      from (select tvid,
                   content_tms_id,
                   content_title,
                   cast(from_unixtime(unix_timestamp(scheduled_content_start_time,"yyyy-MM-dd'T'HH:mm:ss'Z'")) as timestamp) scheduled_content_start_time_utc,
                   network_callsign,
                   content_start_media_time,
                   content_recognition_start content_recognition_start_utc,
                   content_recognition_end content_recognition_end_utc,
                   unix_timestamp(content_recognition_end) - unix_timestamp(content_recognition_start) content_recognition_duration,
                   ipaddress,
                   filedate
            from default.jwillingham_linear_content_original
            where substr(content_tms_id,1,2) in ('EP','MV','SH','SP')) tb1
        join (select tb1.tvid,
                     tb1.hhid,
                     tb1.zipcode,
                     tb1.city,
                     tb1.state,
                     tb2.nielsen_dma_code dma_code
              from default.jwillingham_inscape_dataxu_crosswalk_step3 tb1
                join default.jwillingham_dma_xwalk tb2 on tb1.dma = tb2.inscape_dma_name) tb2 on tb1.tvid = tb2.tvid
        join default.jwillingham_networks_original tb3 on tb1.network_callsign = tb3.station_call_sign) tb0;