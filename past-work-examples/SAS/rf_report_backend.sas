%macro households_intab();
	proc sql;
	create table perm.households_intab as
		select t3.household
		from (select t1.household,
		             t1.weekday,
		             case
		               when t1.intab_days >= t2.threshold then 1
		               else 0
		             end as intab
		      from (select household,
		                   weekday(datepart(intab_starting_date + ('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)))-1 as weekday,
		                   count(intab_starting_date) as intab_days
		            from rshift.&intab_src
		            where intab = 1
		            and   time_period = 0
		            and   datepart(intab_starting_date + ('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end
		            group by household,
		                     weekday) t1
		        join (select weekday,
		                     count(broadcast_day)*.75 as threshold
		              from rshift.broadcast_calendar
		              where datepart(broadcast_day + ('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end
		              group by weekday) t2 on t1.weekday = t2.weekday) t3
		group by t3.household
		having sum(t3.intab) = 7;
	quit;
%mend households_intab;

%macro household_weighting();
	data _null_;
		query="&rshift_bbops. -AF " || '"' || "|" || '"' || " -c " || '"' || "select * from weighting.vwdemographicinfo" || '"' || " > &path.vwdemographicinfo_tmp.txt" || ' && ' || "head -n -1 &path.vwdemographicinfo_tmp.txt > &path.vwdemographicinfo.txt"  || ' && ' || "rm &path.vwdemographicinfo_tmp.txt";
		call system(query);
	run;
	proc import datafile="&path.vwdemographicinfo.txt" out=perm.vwdemographicinfo dbms=dlm replace;
		datarow=2;
		delimiter='|';
		getnames=yes;
	run;
	data _null_;
		query="rm &path.vwdemographicinfo.txt";
		call system(query);
	run;
	data _null_;
		query="&rds_bbprod. -e " || '"' || "select * from weighting.universe_buckets" || '"' || " > &path.universe_buckets.txt";
		call system(query);
	run;
	proc import datafile="&path.universe_buckets.txt" out=perm.universe_buckets dbms=dlm replace;
		datarow=2;
		delimiter='09'x;
		getnames=yes;
		guessingrows=max;
	run;
	data _null_;
		query="rm &path.universe_buckets.txt";
		call system(query);
	run;
	data _null_;
		query="&rds_bbprod. -e " || '"' || "select * from weighting.markets" || '"' || " > &path.markets.txt";
		call system(query);
	run;
	proc import datafile="&path.markets.txt" out=perm.markets dbms=dlm replace;
		datarow=2;
		delimiter='09'x;
		getnames=yes;
		guessingrows=max;
	run;
	data _null_;
		query="rm &path.markets.txt";
		call system(query);
	run;
	proc summary data=perm.vwdemographicinfo;
		var hh_age;
		output out=work.hh_age_mode (drop=_freq_ _type_) mode=age_mode;
	run;
	data _null_;
		set work.hh_age_mode;
		call symputx('age_mode', age_mode);
	run;
	proc summary data=perm.vwdemographicinfo;
		var hh_income;
		output out=work.hh_income_mode (drop=_freq_ _type_) mode=income_mode;
	run;
	data _null_;
		set work.hh_income_mode;
		call symputx('income_mode', income_mode);
	run;
	proc summary data=perm.vwdemographicinfo;
		var hh_size;
		output out=work.hh_size_mode (drop=_freq_ _type_) mode=size_mode;
	run;
	data _null_;
		set work.hh_size_mode;
		call symputx('size_mode', size_mode);
	run;
	proc summary data=perm.vwdemographicinfo;
		var hh_ethnicity;
		output out=work.hh_ethnicity_mode (drop=_freq_ _type_) mode=ethnicity_mode;
	run;
	data _null_;
		set work.hh_ethnicity_mode;
		call symputx('ethnicity_mode', ethnicity_mode);
	run;
	proc sql;
	create table work.vwdemographicinfo as
		select t1.id as household, case hh_age
			when 0 then &age_mode.
			else hh_age
		end as hh_age, case hh_income
			when 0 then &income_mode.
			else hh_income
		end as hh_income, case hh_size
			when 0 then &size_mode.
			else hh_size
		end as hh_size, case hh_ethnicity
			when 0 then &ethnicity_mode.
			else hh_ethnicity
		end as hh_ethnicity, case countycode
			when 'A' then 65
			when 'B' then 66
			when 'C' then 67
			when 'D' then 68
		end as hh_countycode, dma as hh_dma, 1 as hh_weight
		from perm.vwdemographicinfo t1
		join (select household from perm.households_intab) t2
		on t1.id = t2.household;
	quit;
	proc sql;
	create table work.universe_buckets as
		select bucket_type, bmin as type_id, value as percent
		from perm.universe_buckets
		where bucket_type in (1, 2, 3, 4, 7)
		and buckets_group = &bucket.;
	quit;
	proc sql;
	create table work.hh_age as
		select distinct t1.hh_age, t2.percent
		from work.vwdemographicinfo t1
		join work.universe_buckets t2
		on t1.hh_age = t2.type_id
		where t2.bucket_type = 1;
	quit;
	proc sql;
	create table work.hh_income as
		select distinct t1.hh_income, t2.percent
		from work.vwdemographicinfo t1
		join work.universe_buckets t2
		on t1.hh_income = t2.type_id
		where t2.bucket_type = 2;
	quit;
	proc sql;
	create table work.hh_size as
		select distinct t1.hh_size, t2.percent
		from work.vwdemographicinfo t1
		join work.universe_buckets t2
		on t1.hh_size = t2.type_id
		where t2.bucket_type = 3;
	quit;
	proc sql;
	create table work.hh_ethnicity as
		select distinct t1.hh_ethnicity, t2.percent
		from work.vwdemographicinfo t1
		join work.universe_buckets t2
		on t1.hh_ethnicity = t2.type_id
		where t2.bucket_type = 4;
	quit;
	proc sql;
	create table work.hh_countycode as
		select distinct t1.hh_countycode, t2.percent
		from work.vwdemographicinfo t1
		join work.universe_buckets t2
		on t1.hh_countycode = t2.type_id
		where t2.bucket_type = 7;
	quit;
	proc sql;
	create table work.hh_dma as
		select hh_dma, population/sum(population)*100 as percent
		from
			(
			select distinct t1.hh_dma, t2.population
			from work.vwdemographicinfo t1
			join perm.markets t2
			on t1.hh_dma = t2.market
			);
	quit;
	proc summary data=work.vwdemographicinfo;
		var hh_weight;
		output out=work.hh_count (drop=_freq_ _type_) sum=hh_cnt;
	run;
	data _null_;
		set work.hh_count;
		call symputx('cntotal', hh_cnt);
	run;
	%rakinge(inds=work.vwdemographicinfo,
	         outds=work.output,
	         inwt=hh_weight,
	         freqlist=,
	         outwt=raked_wgt,
	         byvar=,
	         varlist=hh_age hh_income hh_size hh_ethnicity hh_countycode hh_dma,
	         numvar=6,
	         cntotal=&cntotal.,
	         trmprec=1,
	         trmpct=,
	         numiter=&iterations.,
	         dircont=work,
	         prdiag=n);
	proc summary data=work.output;
		var raked_wgt;
		output out=work.trim1 (drop=_freq_ _type_) q1=Q1 q3=Q3 qrange=IQR;
	run;
	proc sql;
	create table work.trim2 as
		select Q1-1.5*IQR as floor, Q3+1.5*IQR as ceiling
		from work.trim1;
	quit;
	proc sql;
	create table work.trimmed_weights as
		select *, case
			when raked_wgt>ceiling then ceiling
			when raked_wgt<floor then floor
			else raked_wgt
		end as trimmed_wgt
		from
			(
			select t1.*, floor, ceiling
			from work.output t1
			cross join work.trim2 t2
			);
	quit;
	proc sql;
	create table perm.hh_weights as
		select *, (&projection/sum(trimmed_wgt))*trimmed_wgt as final_wgt
		from work.trimmed_weights;
	quit;
%mend household_weighting;

%macro viewership_data();
	data _null_;
		seconds = '01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt;
		period_start = &period_start;
		call symputx('start_date',dhms(period_start,6,0,0)-seconds);
		period_end = &period_end;
		call symputx('end_date',dhms(period_end,6,0,0)-seconds);
	run;
	data _null_;
		query="&rds_bbprod. -e " || '"' || "select distinct t1.id, t1.name from tra_admin25.networks t1 join tra_admin25.channels t2 on t1.id = t2.network where t2.isactive = 1 and t1.is_national = 1" || '"' || " > &path.networks.txt";
		call system(query);
	run;
	proc import datafile="&path.networks.txt" out=perm.networks dbms=dlm replace;
		datarow=2;
		delimiter='09'x;
		getnames=yes;
		guessingrows=max;
	run;
	data _null_;
		query="rm &path.networks.txt";
		call system(query);
	run;
	data _null_;
		query="&rshift_bbprod. -AF " || '"' || "|" || '"' || " -c " || '"' || "select household, program_airing, broadcast_day, network, cmr_id, campaign, duration from public.ad_viewings where broadcast_day between &start_date and &end_date and campaign in &all_campaigns and live_offset <= &live_offset" || '"' || " > &path.ad_viewings_tmp.txt" || ' && ' || "head -n -1 &path.ad_viewings_tmp.txt > &path.ad_viewings.txt" || ' && ' || "rm &path.ad_viewings_tmp.txt";
		call system(query);
	run;
	proc import datafile="&path.ad_viewings.txt" out=perm.ad_viewings dbms=dlm replace;
		datarow=2;
		delimiter='|';
		getnames=yes;
	run;
	data _null_;
		query="rm &path.ad_viewings.txt";
		call system(query);
	run;
	data _null_;
		query="&rds_bbprod. -e " || '"' || "select cmr_id, case when sum(duration) is null then 0 else sum(duration) end as seconds_aired from (select t3.cmr_id, min(t3.duration) as duration from (select t1.cmr_id, t1.day, t1.duration from tra_admin25.ad_airings as t1 join tra_admin25.campaigns as t2 on t1.campaign = t2.id where t2.id in &all_campaigns) t3 where t3.day between &start_date and &end_date group by t3.cmr_id) tbl group by cmr_id" || '"' || " > &path.seconds_aired.txt";
		call system(query);
	run;
	proc import datafile="&path.seconds_aired.txt" out=perm.seconds_aired dbms=dlm replace;
		datarow=2;
		delimiter='09'x;
		getnames=yes;
	run;
	data _null_;
		query="rm &path.seconds_aired.txt";
		call system(query);
	run;
%mend viewership_data;

%macro demographic_targets();
	%global demo_id;
	%if &target_demos = () %then %do;
			%let demo_id = all;
			data _null_;
				query="&rshift_bbprod. -AF " || '"' || "|" || '"' || " -c " || '"' || "select distinct id from public.households" || '"' || " > &path.hh_demo_tmp.txt" || ' && ' || "head -n -1 &path.hh_demo_tmp.txt > &path.hh_demo.txt" || ' && ' || "rm &path.hh_demo_tmp.txt";
				call system(query);
			run;
			proc import datafile="&path.hh_demo.txt" out=perm.hh_demo_&demo_id dbms=dlm replace;
				datarow=2;
				delimiter='|';
				getnames=yes;
			run;
			data _null_;
				query="rm &path.hh_demo.txt";
				call system(query);
			run;
		%end;
	%else %do;
			data _null_;
				query="&rds_bbprod. -e " || '"' || "select * from tra_admin25.demographic_xwalk" || '"' || " > &path.demographic_xwalk.txt";
				call system(query);
			run;
			proc import datafile="&path.demographic_xwalk.txt" out=perm.demographic_xwalk dbms=dlm replace;
				datarow=2;
				delimiter='09'x;
				getnames=yes;
				guessingrows=max;
			run;
			data _null_;
				query="rm &path.demographic_xwalk.txt";
				call system(query);
			run;
			proc sql;
				select distinct field_name into :field_name from perm.demographic_xwalk where pk_id in &target_demos;
				select distinct bit_demarcation format best32. into :bit_demarcation from perm.demographic_xwalk where pk_id in &target_demos;
				select distinct bit_value format best32. into :bit_value separated by ',' from perm.demographic_xwalk where pk_id in &target_demos;
				select distinct pk_id into :demo_id separated by 'or' from perm.demographic_xwalk where pk_id in &target_demos;
			quit;
			data _null_;
				query="&rshift_bbprod. -AF " || '"' || "|" || '"' || " -c " || '"' || "select distinct id from public.households where &field_name & &bit_demarcation in (&bit_value)" || '"' || " > &path.hh_demo_tmp.txt" || ' && ' || "head -n -1 &path.hh_demo_tmp.txt > &path.hh_demo.txt" || ' && ' || "rm &path.hh_demo_tmp.txt";
				call system(query);
			run;
			proc import datafile="&path.hh_demo.txt" out=perm.hh_demo_&demo_id dbms=dlm replace;
				datarow=2;
				delimiter='|';
				getnames=yes;
			run;
			data _null_;
				query="rm &path.hh_demo.txt";
				call system(query);
			run;
		%end;
%mend demographic_targets;

%macro rf_grp_period();
	proc sql;
	create table perm.rf_grp_period as
		select &sample as sample,
		       &period_start format date9. as period_start,
		       &period_end format date9. as period_end,
		       t1.hh_intab_unw,
		       t1.hh_intab_wgt,
		       t2.hh_tuned_unw as reach_unw,
		       t2.hh_tuned_wgt as reach_wgt,
		       t2.hh_tuned_unw/t1.hh_intab_unw as pct_reach_unw,
		       t2.hh_tuned_wgt/t1.hh_intab_wgt as pct_reach_wgt,
		       t3.tot_views_unw/t2.hh_tuned_unw as avg_freq_unw,
		       t3.tot_views_wgt/t2.hh_tuned_wgt as avg_freq_wgt,
               t4.grp
		from (select 0 as id,
		             count(*) as hh_intab_unw,
		             case
		               when sum(final_wgt) is null then 0
		               else sum(final_wgt)
		             end as hh_intab_wgt
		      from perm.hh_weights hit
		        join rshift.households hh on hit.household = hh.id
		        join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
		      where mso in &mso) t1
		  join (select 0 as id,
		               count(*) as hh_tuned_unw,
		               sum(final_wgt) as hh_tuned_wgt
		        from (select distinct av.household,
		                     max(final_wgt) as final_wgt
		              from perm.ad_viewings av
		                left join rshift.original_program_airings opa on opa.program_airing = av.program_airing
		                join rshift.households hh on hh.id = av.household
		                join perm.hh_weights hit on av.household = hit.household
		                join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
		              where datepart(av.broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end
		              and   campaign in &target_campaigns
		              and   av.network in (select id from perm.networks)
		              and   mso in &mso
		              group by av.household) tbl) t2 on t1.id = t2.id
		  join (select 0 as id,
		               sum(viewings*households_unw) as tot_views_unw,
		               sum(viewings*households_wgt) as tot_views_wgt
		        from (select case
		                       when viewings is null then 0
		                       else viewings
		                     end as viewings,
		                     sum(hit.final_wgt) as households_wgt,
		                     count(*) as households_unw
		              from perm.hh_weights hit
		                join rshift.households hh on hh.id = hit.household
		                join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
		                left join (select av.household,
		                                  max(final_wgt) as final_wgt,
		                                  count(distinct av.cmr_id) as viewings
		                           from perm.ad_viewings av
		                             join rshift.households hh on hh.id = av.household
		                             join perm.hh_weights hit on av.household = hit.household
		                           where datepart(av.broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end
		                           and   av.network in (select id from perm.networks)
		                           and   mso in &mso
		                           and   campaign in &target_campaigns
		                           group by av.household) hv on hv.household = hit.household
		              where mso in &mso
		              group by viewings) tbl) t3 on t1.id = t3.id
		  join (select 0 as id,
		               sum(s2.seconds_tuned/(s1.hh_intab_wgt*s3.seconds_aired))*100 as grp
		        from (select 0 as id,
		                     case
		                       when sum(final_wgt) is null then 0
		                       else sum(final_wgt)
		                     end as hh_intab_wgt
		              from perm.hh_weights hit
		                join rshift.households hh on hit.household = hh.id
		                join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
		              where mso in &mso) s1
		          join (select 0 as id,
		                       cmr_id,
		                       sum(duration*final_wgt) as seconds_tuned
		                from perm.ad_viewings av
		                  join rshift.households hh on hh.id = av.household
		                  join perm.hh_weights hit on av.household = hit.household
		                  join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
		                where datepart(broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end
		                and   mso in &mso
		                and   campaign in &target_campaigns
		                group by cmr_id) s2 on s1.id = s2.id
		          join (select 0 as id, cmr_id, seconds_aired from perm.seconds_aired) s3
		            on s1.id = s3.id
		           and s2.cmr_id = s3.cmr_id) t4 on t1.id = t4.id;
	quit;
	proc export data=perm.rf_grp_period outfile="&path.rf_grp_period_&stamp..csv" dbms=csv replace; run;
%mend rf_grp_period;

%macro reach_wk();
	proc sql;
	create table work.temp_calendar as
		select monotonic() as week,
		       week_start,
		       week_end
		from (select distinct datepart(broadcast_week+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) format date9. as week_start,
		             intnx('day',datepart(broadcast_week+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)),6) format date9. as week_end
		      from rshift.broadcast_calendar
		      where datepart(broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end);
	quit;
	data _null_;
		set work.temp_calendar end=end;
		count+1;
		if end then call symputx('max',count);
	run;
	%do i=1 %to &max.;
		proc sql;
		create table work.reach_wk&i as
			select &sample as sample,
			       &i as week,
			       (select week_start from work.temp_calendar where week = &i) format date9. as week_start,
			       (select week_end from work.temp_calendar where week = &i) format date9. as week_end,
			       count(*) as reach_unw,
			       sum(final_wgt) as reach_wgt
			from (select distinct av.household,
			             max(final_wgt) as final_wgt
			      from perm.ad_viewings av
			        left join rshift.original_program_airings opa on opa.program_airing = av.program_airing
			        join rshift.households hh on hh.id = av.household
			        join rshift.broadcast_calendar bc on bc.broadcast_day = av.broadcast_day
			        join perm.hh_weights hit on av.household = hit.household
					join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
			      where datepart(av.broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between (select week_start from work.temp_calendar where week = &i) and (select week_end from work.temp_calendar where week = &i)
			      and   campaign in &target_campaigns
			      and   av.network in (select id from perm.networks)
			      and   mso in &mso
			      group by av.household) tbl;
		quit;
	%end;
	data perm.reach_wk;
		set work.reach_wk1-work.reach_wk&max;
	run;
	proc export data=perm.reach_wk outfile="&path.reach_wk_&stamp..csv" dbms=csv replace; run;
%mend reach_wk;

%macro cum_reach_wk();
	proc sql;
	create table work.temp_calendar as
		select monotonic() as week,
		       week_start,
		       week_end
		from (select distinct datepart(broadcast_week+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) format date9. as week_start,
		             intnx('day',datepart(broadcast_week+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)),6) format date9. as week_end
		      from rshift.broadcast_calendar
		      where datepart(broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and &period_end);
	quit;
	data _null_;
		set work.temp_calendar end=end;
		count+1;
		if end then call symputx('max',count);
	run;
	%do i=1 %to &max.;
		proc sql;
		create table work.cum_reach_wk&i as
			select &sample as sample,
			       &i as week,
			       &period_start format date9. as week_start,
			       (select week_end from work.temp_calendar where week = &i) format date9. as week_end,
			       count(*) as cum_reach_unw,
			       sum(final_wgt) as cum_reach_wgt
			from (select distinct av.household,
			             max(final_wgt) as final_wgt
			      from perm.ad_viewings av
			        left join rshift.original_program_airings opa on opa.program_airing = av.program_airing
			        join rshift.households hh on hh.id = av.household
			        join rshift.broadcast_calendar bc on bc.broadcast_day = av.broadcast_day
			        join perm.hh_weights hit on av.household = hit.household
					join perm.hh_demo_&demo_id hhd on hit.household = hhd.id
			      where datepart(av.broadcast_day+('01JAN2000:06:00:00'dt-'01JAN1960:06:00:00'dt)) between &period_start and (select week_end from work.temp_calendar where week = &i)
			      and   campaign in &target_campaigns
			      and   av.network in (select id from perm.networks)
			      and   mso in &mso
			      group by av.household) tbl;
		quit;
	%end;
	data perm.cum_reach_wk;
		set work.cum_reach_wk1-work.cum_reach_wk&max;
	run;
	proc export data=perm.cum_reach_wk outfile="&path.cum_reach_wk_&stamp..csv" dbms=csv replace; run;
%mend cum_reach_wk;