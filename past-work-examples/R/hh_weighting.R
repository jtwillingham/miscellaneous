# Purpose - Derive Weighted Intab Households for Custom Broadcast Period
# Author - Jeff Willingham
# Created - 12/07/2016
# Modified - 05/02/2017

# DERIVE DAILY INTAB
daily_intab <- FALSE

# MAXIMUM NUMBER OF ITERATIONS
wgt_iterations <- 100

# MAXIMUM ABSOLUTE DIFFERENCE IN WEIGHTS ALLOWED BETWEEN ITERATIONS
wgt_tolerance <- 1e-10

# KNOWN HOUSEHOLD POPULATION BEING PROJECTED TO
projection <- 114649000

# NUMBER OF ATTRIBUTES BEING WEIGHTED TO
num_attributes <- 6

# IDENTIFIER FOR THE BUCKET OF KNOWN HOUSEHOLD MARGINS OF DEMOGRAPHIC ATTRIBUTES BEING WEIGHTED TO
buckets_grp <- 100

# UNIVERSE OF DAILY INTAB HOUSEHOLDS
intab_src <- "public.households_intab_national"

# OUTPUT FOLDER PATH
output_dir <- "/path/to/output/"

# FIRST AND LAST DATES OF THE BROADCAST PERIOD
period_start <- "YYYY-MM-DD"
period_end <- "YYYY-MM-DD"

# 1 = Charter
# 3 = TiVo
# 4 = FourthWall
# 5 = Cox
# 6 = Rovi
mso_src <- "(1,3,4,5,6)"

# NAME OF INTAB TABLE TO BE CREATED
output_tbl <- "output_table_name"

# 'None' = This does not trim the weights outputted by the iterative proportional fitting procedure.
# '100x' = This is the current weight-trimming method used in Media TRAnalytics.
# 'IQR' = This trims weights using Tukey's IQR method for outlier detection.
# 'UTL' = This trims weights using the upper tolerance limit of a lognormal tolerance interval (95% confidence, 95% proportion).
trim_method <- "100x"

# RDS (BBOPS) USERNAME AND PASSWORD
rds_username <- "username"
rds_password <- "password"

# REDSHIFT (BBOPS) USERNAME AND PASSWORD
rshift_username <- "username"
rshift_password <- "password"

# AWS ACCESS KEYS
aws_access_key_id <- "aws_access_key_id"
aws_secret_access_key <- "aws_secret_access_key"

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
# DO NOT ALTER THE CODE BELOW UNLESS YOU KNOW WHAT YOU'RE DOING #
#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

func_seconds <-
  function(date_str)
  {
    epoch_diff1 <- as.integer(as.POSIXct("2000-01-01 00:00:00", tz = "UTC")) - as.integer(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"))
    epoch_diff2 <- as.integer(as.POSIXct(date_str, tz = "UTC"))+6*60*60 - as.integer(as.POSIXct("1970-01-01 00:00:00", tz = "UTC"))
    out <- epoch_diff2 - epoch_diff1
    return(out)
  }

# Calculate a one-sided, upper tolerance interval.
# Tolerance factor based on the non-central t distribution; further modified for a lognormal distribution.
# source = http://www.itl.nist.gov/div898/handbook/prc/section2/prc263.htm
# source = https://github.com/cran/tolerance/blob/master/R/normtolint.R
func_lognormal_utlimit <-
  function(variable, confidence, proportion)
  {
    x <- log(variable)
    n <- length(x)
    f <- n - 1
    x_bar <- mean(x)
    std <- sd(x)
    delta <- qnorm(proportion)*sqrt(n)
    k <- qt(confidence,f,delta)/sqrt(n)
    utlimit <- exp(x_bar+k*std)
    return(utlimit)
  }

if (daily_intab == FALSE){
  diff_days <- as.integer(as.Date(period_end))+1 - as.integer(as.Date(period_start))
  if (diff_days >= 7 & diff_days %% 7 == 0){

    options(warn = -1)
    my_packages <- c("dplyr", "magrittr", "RMySQL", "RPostgreSQL")
    lapply(my_packages, install.packages, repos = "https://cran.rstudio.com/", quiet = TRUE)
    suppressPackageStartupMessages(lapply(my_packages, library, character.only = TRUE, quietly = TRUE))

    rds_bbops <-
      src_mysql(
        dbname = 'bbops',
        host = '10.225.5.32',
        port = 3306,
        user = rds_username,
        password = rds_password)

    rshift_bbops <-
      src_postgres(
        dbname = 'production',
        host = '10.225.5.51',
        port = 5439,
        user = rshift_username,
        password = rshift_password)

    hh_age_mode <-
      tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
      group_by(hh_age) %>%
      summarise(cnt = n()) %>%
      filter(cnt == max(cnt)) %>%
      collect(n = Inf) %$%
      hh_age

    hh_income_mode <-
      tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
      group_by(hh_income) %>%
      summarise(cnt = n()) %>%
      filter(cnt == max(cnt)) %>%
      collect(n = Inf) %$%
      hh_income

    hh_size_mode <-
      tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
      group_by(hh_size) %>%
      summarise(cnt = n()) %>%
      filter(cnt == max(cnt)) %>%
      collect(n = Inf) %$%
      hh_size

    hh_ethnicity_mode <-
      tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
      group_by(hh_ethnicity) %>%
      summarise(cnt = n()) %>%
      filter(cnt == max(cnt)) %>%
      collect(n = Inf) %$%
      hh_ethnicity

    if (diff_days == 7)
    {
      vwdemographicinfo <-
        tbl(rshift_bbops, sql(sprintf("
        select id,
               case hh_age
                 when 0 then %s
                 else hh_age
               end as hh_age,
               case hh_income
                 when 0 then %s
                 else hh_income
               end as hh_income,
               case hh_size
                 when 0 then %s
                 else hh_size
               end as hh_size,
               case hh_ethnicity
                 when 0 then %s
                 else hh_ethnicity
               end as hh_ethnicity,
               case countycode
                 when 'A' then 65
                 when 'B' then 66
                 when 'C' then 67
                 when 'D' then 68
               end as hh_countycode,
               dma as hh_dma
        from weighting.vwdemographicinfo_ds t1
          join (select household
                from %s t2
                  join public.households t3 on t2.household = t3.id
                where mso in %s
                and   intab = 1
                and   time_period = 0
                and   intab_starting_date between %s and %s
                group by household
                having count(household) >= 7*.75) t4 on t1.id = t4.household", hh_age_mode, hh_income_mode, hh_size_mode, hh_ethnicity_mode, intab_src, mso_src, func_seconds(period_start), func_seconds(period_end)))) %>%
        collect(n = Inf)
    }
    if (diff_days > 7)
    {
      vwdemographicinfo <-
        tbl(rshift_bbops, sql(sprintf("
        select id,
               case hh_age
                 when 0 then %s
                 else hh_age
               end as hh_age,
               case hh_income
                 when 0 then %s
                 else hh_income
               end as hh_income,
               case hh_size
                 when 0 then %s
                 else hh_size
               end as hh_size,
               case hh_ethnicity
                 when 0 then %s
                 else hh_ethnicity
               end as hh_ethnicity,
               case countycode
                 when 'A' then 65
                 when 'B' then 66
                 when 'C' then 67
                 when 'D' then 68
               end as hh_countycode,
               dma as hh_dma
        from weighting.vwdemographicinfo_ds t1
          join (select t4.household
                from (select t2.household,
                             t2.weekday,
                             case
                               when t2.intab_days >= t3.threshold then 1
                               else 0
                             end as intab
                      from (select household,
                                   extract(weekday from f_cdt(intab_starting_date)) as weekday,
                                   count(intab_starting_date) as intab_days
                            from public.households hh
                              join %s hit on hh.id = hit.household
                            where mso in %s
                            and   intab = 1
                            and   time_period = 0
                            and   intab_starting_date between %s and %s
                            group by household,
                                     weekday) t2
                        join (select weekday,
                                     count(broadcast_day)*.75 as threshold
                              from public.broadcast_calendar
                              where broadcast_day between %s and %s
                              group by weekday) t3 on t2.weekday = t3.weekday) t4
                group by t4.household
                having sum(t4.intab) = 7) t5 on t1.id = t5.household", hh_age_mode, hh_income_mode, hh_size_mode, hh_ethnicity_mode, intab_src, mso_src, func_seconds(period_start), func_seconds(period_end), func_seconds(period_start), func_seconds(period_end)))) %>% 
        collect(n = Inf)
    }

    universe_buckets <-
      tbl(rds_bbops, sql("select * from weighting.universe_buckets")) %>%
      filter(BUCKETS_GROUP == buckets_grp & BUCKET_TYPE %in% c(1, 2, 3, 4, 7)) %>%
      select(BUCKET_TYPE, BMIN, VALUE) %>%
      collect(n = Inf)
    names(universe_buckets) <-
      names(universe_buckets) %>%
      tolower()

    markets <-
      tbl(rds_bbops, sql("select * from weighting.markets")) %>%
      collect(n = Inf)
    names(markets) <-
      names(markets) %>%
      tolower()

    hh_age_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_age" = "bmin")) %>%
      filter(bucket_type == 1) %>%
      select(hh_age, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_age, percent)

    hh_income_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_income" = "bmin")) %>%
      filter(bucket_type == 2) %>%
      select(hh_income, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_income, percent)

    hh_size_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_size" = "bmin")) %>%
      filter(bucket_type == 3) %>%
      select(hh_size, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_size, percent)

    hh_ethnicity_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_ethnicity" = "bmin")) %>%
      filter(bucket_type == 4) %>%
      select(hh_ethnicity, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_ethnicity, percent)

    hh_countycode_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_countycode" = "bmin")) %>%
      filter(bucket_type == 7) %>%
      select(hh_countycode, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_countycode, percent)

    hh_dma_df <-
      vwdemographicinfo %>%
      inner_join(markets, by = c("hh_dma" = "market")) %>%
      select(hh_dma, value = population) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_dma, percent)

    samp_data <-
      vwdemographicinfo %>%
      mutate(weight = 1)
    cntr_total <-
      samp_data %>%
      summarise(cntr_total = n()) %$%
      cntr_total
    target_data <- list(hh_age_df, hh_income_df, hh_size_df, hh_ethnicity_df, hh_countycode_df, hh_dma_df)
    target_list <- c("hh_age", "hh_income", "hh_size", "hh_ethnicity", "hh_countycode", "hh_dma")

    result <- samp_data
    for (i in seq(wgt_iterations))
    {
      result_temp <- result
      for (j in seq(num_attributes))
      {
        margin_input <-
          result %>%
          group_by_(.dots = target_list[j]) %>%
          summarise(samp_margin = sum(weight))
        result <-
          result %>%
          inner_join(target_data[[j]], by = target_list[j]) %>%
          inner_join(margin_input, by = target_list[j]) %>%
          mutate(margin_est = cntr_total*percent,
                 adj_factor = margin_est/samp_margin,
                 weight = weight*adj_factor) %>%
          select(id:hh_dma, weight)
      }
      stop_crit <- max(abs(result[, "weight"] - result_temp[, "weight"]))
      if (stop_crit < wgt_tolerance)
      {
        cat('       stopping criterion:', stop_crit, '\n')
        cat('Convergence reached after', i, 'iterations!\n')
        break
      }
    }

    if (trim_method == 'None')
    {
      output_df <-
        result %>%
        mutate(hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, hh_weight)
    }
    if (trim_method == '100x')
    {
      output_df <-
        result %>%
        mutate(weight = if_else(weight < 0.1, 0.1, if_else(weight > 10, 10, weight)),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, hh_weight)
    }
    if (trim_method == 'IQR')
    {
      output_df <-
        result %>%
        mutate(lower_bound = quantile(weight, probs = .25)-1.5*IQR(weight),
               upper_bound = quantile(weight, probs = .75)+1.5*IQR(weight),
               weight = if_else(weight < lower_bound, lower_bound, if_else(weight > upper_bound, upper_bound, weight)),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, hh_weight)
    }
    if (trim_method == 'UTL')
    {
      output_df <- 
        result %>%
        mutate(ut_limit = func_lognormal_utlimit(weight, 0.95, 0.95),
               weight = if_else(weight > ut_limit, ut_limit, weight),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, hh_weight)
    }

    output_name <- paste(output_dir, "hh_weights.txt", sep = "")
    write.table(output_df, output_name, sep = "|", na = "", row.names = FALSE, col.names = FALSE)

    bucket_suffix <- output_tbl
    output_dir_bash <- gsub(" ", "\\ ", output_dir, fixed = TRUE)
    bucket_command1 <- sprintf("aws s3 mb s3://datascience_%s", bucket_suffix)
    bucket_command2 <- sprintf("aws s3 cp %shh_weights.txt s3://datascience_%s", output_dir_bash, bucket_suffix)
    bucket_command3 <- sprintf("aws s3 rb s3://datascience_%s --force", bucket_suffix)
    str1 <- sprintf("export PGPASSWORD='%s' && psql -h 10.225.5.51 -p 5439 -d production -U %s -c", rshift_password, rshift_username)
    str2 <- shQuote(sprintf("drop table if exists datascience.%s_%s; commit; create table datascience.%s_%s (household integer, weight float8) distkey (household) sortkey (household); commit; copy datascience.%s_%s from 's3://datascience_%s/hh_weights.txt' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'; commit;", rshift_username, output_tbl, rshift_username, output_tbl, rshift_username, output_tbl, bucket_suffix, aws_access_key_id, aws_secret_access_key))
    psql_command <- paste(str1, str2, sep = " ")
    system(bucket_command1)
    system(bucket_command2)
    system(psql_command)
    system(bucket_command3)

  } else {cat('BROADCAST PERIOD MUST BE IN WEEKLY INCREMENTS')}
} else {

  options(warn = -1)
  my_packages <- c("dplyr", "magrittr", "RMySQL", "RPostgreSQL")
  lapply(my_packages, install.packages, repos = "https://cran.rstudio.com/", quiet = TRUE)
  suppressPackageStartupMessages(lapply(my_packages, library, character.only = TRUE, quietly = TRUE))

  rds_bbops <-
    src_mysql(
      dbname = 'bbops',
      host = '10.225.5.32',
      port = 3306,
      user = rds_username,
      password = rds_password)

  rshift_bbops <-
    src_postgres(
      dbname = 'production',
      host = '10.225.5.51',
      port = 5439,
      user = rshift_username,
      password = rshift_password)

  hh_age_mode <-
    tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
    group_by(hh_age) %>%
    summarise(cnt = n()) %>%
    filter(cnt == max(cnt)) %>%
    collect(n = Inf) %$%
    hh_age

  hh_income_mode <-
    tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
    group_by(hh_income) %>%
    summarise(cnt = n()) %>%
    filter(cnt == max(cnt)) %>%
    collect(n = Inf) %$%
    hh_income

  hh_size_mode <-
    tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
    group_by(hh_size) %>%
    summarise(cnt = n()) %>%
    filter(cnt == max(cnt)) %>%
    collect(n = Inf) %$%
    hh_size

  hh_ethnicity_mode <-
    tbl(rshift_bbops, sql("select * from weighting.vwdemographicinfo_ds")) %>%
    group_by(hh_ethnicity) %>%
    summarise(cnt = n()) %>%
    filter(cnt == max(cnt)) %>%
    collect(n = Inf) %$%
    hh_ethnicity

  intab_days <-
    tbl(rshift_bbops, sql(sprintf("
    select intab_starting_date
    from %s
    where intab = 1
    and   time_period = 0
    and   intab_starting_date between %s and %s
    group by intab_starting_date", intab_src, func_seconds(period_start), func_seconds(period_end)))) %>%
    collect(n = Inf) %>%
    arrange(intab_starting_date)

  for (x in 1:nrow(intab_days))
    {

    intab_start_date <- intab_days[x, 1]

    vwdemographicinfo <-
      tbl(rshift_bbops, sql(sprintf("
      select id,
             case hh_age
               when 0 then %s
               else hh_age
             end as hh_age,
             case hh_income
               when 0 then %s
               else hh_income
             end as hh_income,
             case hh_size
               when 0 then %s
               else hh_size
             end as hh_size,
             case hh_ethnicity
               when 0 then %s
               else hh_ethnicity
             end as hh_ethnicity,
             case countycode
               when 'A' then 65
               when 'B' then 66
               when 'C' then 67
               when 'D' then 68
             end as hh_countycode,
             dma as hh_dma
      from weighting.vwdemographicinfo_ds t1
        join (select household
              from %s t2
                join public.households t3 on t2.household = t3.id
              where mso in %s
              and   intab = 1
              and   time_period = 0
              and   intab_starting_date = %s
              group by household) t4 on t1.id = t4.household", hh_age_mode, hh_income_mode, hh_size_mode, hh_ethnicity_mode, intab_src, mso_src, intab_start_date))) %>%
      collect(n = Inf)

    universe_buckets <-
      tbl(rds_bbops, sql("select * from weighting.universe_buckets")) %>%
      filter(BUCKETS_GROUP == buckets_grp & BUCKET_TYPE %in% c(1, 2, 3, 4, 7)) %>%
      select(BUCKET_TYPE, BMIN, VALUE) %>%
      collect(n = Inf)
    names(universe_buckets) <-
      names(universe_buckets) %>%
      tolower()

    markets <-
      tbl(rds_bbops, sql("select * from weighting.markets")) %>%
      collect(n = Inf)
    names(markets) <-
      names(markets) %>%
      tolower()

    hh_age_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_age" = "bmin")) %>%
      filter(bucket_type == 1) %>%
      select(hh_age, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_age, percent)

    hh_income_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_income" = "bmin")) %>%
      filter(bucket_type == 2) %>%
      select(hh_income, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_income, percent)

    hh_size_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_size" = "bmin")) %>%
      filter(bucket_type == 3) %>%
      select(hh_size, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_size, percent)

    hh_ethnicity_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_ethnicity" = "bmin")) %>%
      filter(bucket_type == 4) %>%
      select(hh_ethnicity, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_ethnicity, percent)

    hh_countycode_df <-
      vwdemographicinfo %>%
      inner_join(universe_buckets, by = c("hh_countycode" = "bmin")) %>%
      filter(bucket_type == 7) %>%
      select(hh_countycode, value) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_countycode, percent)

    hh_dma_df <-
      vwdemographicinfo %>%
      inner_join(markets, by = c("hh_dma" = "market")) %>%
      select(hh_dma, value = population) %>%
      distinct() %>%
      mutate(percent = value/sum(value)) %>%
      select(hh_dma, percent)

    samp_data <-
      vwdemographicinfo %>%
      mutate(weight = 1)
    cntr_total <-
      samp_data %>%
      summarise(cntr_total = n()) %$%
      cntr_total
    target_data <- list(hh_age_df, hh_income_df, hh_size_df, hh_ethnicity_df, hh_countycode_df, hh_dma_df)
    target_list <- c("hh_age", "hh_income", "hh_size", "hh_ethnicity", "hh_countycode", "hh_dma")

    result <- samp_data
    for (i in seq(wgt_iterations))
    {
      result_temp <- result
      for (j in seq(num_attributes))
      {
        margin_input <-
          result %>%
          group_by_(.dots = target_list[j]) %>%
          summarise(samp_margin = sum(weight))
        result <-
          result %>%
          inner_join(target_data[[j]], by = target_list[j]) %>%
          inner_join(margin_input, by = target_list[j]) %>%
          mutate(margin_est = cntr_total*percent,
                 adj_factor = margin_est/samp_margin,
                 weight = weight*adj_factor) %>%
          select(id:hh_dma, weight)
      }
      stop_crit <- max(abs(result[, "weight"] - result_temp[, "weight"]))
      if (stop_crit < wgt_tolerance)
      {
        cat('       stopping criterion:', stop_crit, '\n')
        cat('Convergence reached after', i, 'iterations!\n')
        break
      }
    }

    if (trim_method == 'None')
    {
      output_df <-
        result %>%
        mutate(intab_starting_date = as.integer(intab_start_date),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, intab_starting_date, hh_weight)
    }
    if (trim_method == '100x')
    {
      output_df <-
        result %>%
        mutate(intab_starting_date = as.integer(intab_start_date),
               weight = if_else(weight < 0.1, 0.1, if_else(weight > 10, 10, weight)),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, intab_starting_date, hh_weight)
    }
    if (trim_method == 'IQR')
    {
      output_df <-
        result %>%
        mutate(intab_starting_date = as.integer(intab_start_date),
               lower_bound = quantile(weight, probs = .25)-1.5*IQR(weight),
               upper_bound = quantile(weight, probs = .75)+1.5*IQR(weight),
               weight = if_else(weight < lower_bound, lower_bound, if_else(weight > upper_bound, upper_bound, weight)),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, intab_starting_date, hh_weight)
    }
    if (trim_method == 'UTL')
    {
      output_df <-
        result %>%
        mutate(intab_starting_date = as.integer(intab_start_date),
               ut_limit = func_lognormal_utlimit(weight, 0.95, 0.95),
               weight = if_else(weight > ut_limit, ut_limit, weight),
               hh_weight = projection/sum(weight)*weight) %>%
        select(household = id, intab_starting_date, hh_weight)
    }

    output_name <- paste(output_dir, "hh_weights.txt", sep = "")
    write.table(output_df, output_name, sep = "|", na = "", row.names = FALSE, col.names = FALSE)

    bucket_suffix <- output_tbl
    output_dir_bash <- gsub(" ", "\\ ", output_dir, fixed = TRUE)
    bucket_command1 <- sprintf("aws s3 mb s3://datascience_%s", bucket_suffix)
    bucket_command2 <- sprintf("aws s3 cp %shh_weights.txt s3://datascience_%s", output_dir_bash, bucket_suffix)
    bucket_command3 <- sprintf("aws s3 rb s3://datascience_%s --force", bucket_suffix)
    str1 <- sprintf("export PGPASSWORD='%s' && psql -h 10.225.5.51 -p 5439 -d production -U %s -c", rshift_password, rshift_username)
    if (x == 1)
    {
      str2 <- shQuote(sprintf("drop table if exists datascience.%s_%s; commit; create table datascience.%s_%s (household integer, intab_starting_date integer, weight float8) distkey (household) sortkey (household); commit; copy datascience.%s_%s from 's3://datascience_%s/hh_weights.txt' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'; commit;", rshift_username, output_tbl, rshift_username, output_tbl, rshift_username, output_tbl, bucket_suffix, aws_access_key_id, aws_secret_access_key))
    }
    if (x > 1)
    {
      str2 <- shQuote(sprintf("copy datascience.%s_%s from 's3://datascience_%s/hh_weights.txt' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'; commit;", rshift_username, output_tbl, bucket_suffix, aws_access_key_id, aws_secret_access_key))
    }
    psql_command <- paste(str1, str2, sep = " ")
    system(bucket_command1)
    system(bucket_command2)
    system(psql_command)
    system(bucket_command3)

    }
}