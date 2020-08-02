drop table if exists default.jwillingham_linear_content_sourced_original;

create external table default.jwillingham_linear_content_sourced_original
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
  `viewing_source` string
  )
partitioned by (`filedate` date)
row format delimited fields terminated by ','
location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/';

alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-01') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-01-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-02') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-02-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-03') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-03-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-04') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-04-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-05') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-05-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-06') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-06-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-07') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-07-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-08') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-08-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-09') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-09-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-10') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-10-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-11') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-11-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-12') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-12-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-13') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-13-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-14') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-14-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-15') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-15-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-16') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-16-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-17') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-17-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-18') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-18-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-19') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-19-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-20') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-20-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-21') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-21-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-22') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-22-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-23') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-23-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-24') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-24-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-25') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-25-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-26') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-26-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-27') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-27-07/';
alter table default.jwillingham_linear_content_sourced_original add partition (filedate = '2018-02-28') location 's3://dxinnovation-useast1-dev/tv/acr/contentconsumption/timeshifted/2018-02-28-07/';

drop table if exists default.jwillingham_linear_content_sourced_test;

create table default.jwillingham_linear_content_sourced_test as
select distinct hhid,
       tvid,
       zipcode,
       dma,
       state,
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
       date_format(from_unixtime(unix_timestamp(content_recognition_start_normalized) - (unix_timestamp(content_recognition_start_normalized) % 1800)),'HH:mm') half_hour,
       date_format(content_recognition_start_normalized,'u') dayofweek,
       live_offset,
       round(live_offset - content_start_media_time/1e+3) content_start_media_time_minus_live_offset,
       viewing_source,
       filedate
from (select tb1.*,
             tb2.hhid,
             tb2.state,
             tb3.station_call_sign,
             tb3.network_name,
             tb3.national_network_flag,
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
                   zipcode,
                   dma,
                   content_tms_id,
                   content_title,
                   cast(from_unixtime(unix_timestamp(scheduled_content_start_time,"yyyy-MM-dd'T'HH:mm:ss'Z'")) as timestamp) scheduled_content_start_time_utc,
                   network_callsign,
                   content_start_media_time,
                   content_recognition_start content_recognition_start_utc,
                   content_recognition_end content_recognition_end_utc,
                   unix_timestamp(content_recognition_end) - unix_timestamp(content_recognition_start) content_recognition_duration,
                   viewing_source,
                   filedate
            from default.jwillingham_linear_content_sourced_original
            where viewing_source in ('dvr','live')) tb1
        join default.tv_inscape_ip_to_tv_with_hh_consolidated tb2 on tb1.tvid = tb2.tvid
        join default.jwillingham_networks_original tb3 on tb1.network_callsign = tb3.station_call_sign) tb0;