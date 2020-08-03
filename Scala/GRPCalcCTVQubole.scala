import org.apache.spark.sql.DataFrame

val ctvPanel: String = "default.jwillingham_ctv_panel_weighted_weekly_20180129_20180225"
val ctvTransactions: String = "default.jwillingham_ctv_transactions_step5"
val ctvHouseholds: String = "default.jwillingham_household_demographics_step5"

val flightSelection: String = "0Ftpw2fCf8"
val targetName: String = "New York, All Households"
val timePeriod: Int = 1 // 0 = day, 1 = week, 2 = month, 3 = quarter
val intabStartDate: String = "2018-02-12"
val intabEndDate: String = "2018-02-18"
val localNational: Int = 1 // 0 = National, 1 = Local
val ageSelection: String = "(1, 2, 3)" // 1 = 18 - 34, 2 = 35 - 54, 3 = 55 +
val incomeSelection: String = "(1, 2, 3, 4)" // 1 = < $20,000, 2 = $20,000 - $49,999, 3 = $50,000 - $99,999, 4 = $100,000 +
val sizeSelection: String = "(1, 2, 3)" // 1 = 1 - 2, 2 = 3 - 4, 3 = 5 +
val raceSelection: String = "(1, 2, 3, 4)" // 1 = Asian, 2 = Black, 3 = White, 4 = Hispanic
val populationDensitySelection: String = "(1, 2, 3, 4, 5, 6, 7)"
val dmaSelection: String = "18766"

val flightInfoDF: DataFrame = spark.sqlContext.sql(
  s"""
     |select 0 id,
     |       tb1.name advertiser,
     |       tb2.name campaign,
     |       tb3.description flight,
     |       concat(cast(cast(tb3.start_at as date) as string), " – " , cast(cast(tb3.end_at as date) as string)) flight_period
     |from default.core_advertisers tb1
     |join default.core_campaigns tb2 on tb1.id = tb2.advertiser_id
     |join default.core_flights tb3 on tb2.id = tb3.campaign_id
     |where tb3.uid = '$flightSelection'""".stripMargin)

def availableHouseholds(inputValue: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputValue == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.household_pel,
         |             tb1.projected_weight
         |      from $ctvPanel tb1
         |        join $ctvHouseholds tb2 on tb1.household_pel = tb2.household_pel
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate'
         |      and   hh_age_code in $ageSelection
         |      and   hh_income_code in $incomeSelection
         |      and   hh_size_code in $sizeSelection
         |      and   hh_race_code in $raceSelection
         |      and   population_density_code in $populationDensitySelection)""".stripMargin)
  }
  if (inputValue == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.household_pel,
         |             tb1.projected_weight
         |      from $ctvPanel tb1
         |        join $ctvHouseholds tb2 on tb1.household_pel = tb2.household_pel
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate'
         |      and   hh_age_code in $ageSelection
         |      and   hh_income_code in $incomeSelection
         |      and   hh_size_code in $sizeSelection
         |      and   hh_race_code in $raceSelection
         |      and   population_density_code in $populationDensitySelection
         |      and   dma_code in ($dmaSelection))""".stripMargin)
  }
  outputDF
}

def householdsViewed(inputValue: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputValue == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select distinct tb1.household_pel,
         |             tb1.projected_weight
         |      from $ctvPanel tb1
         |        join $ctvHouseholds tb2 on tb1.household_pel = tb2.household_pel
         |        join $ctvTransactions tb3 on tb1.household_pel = tb3.household_pel
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate'
         |      and   hh_age_code in $ageSelection
         |      and   hh_income_code in $incomeSelection
         |      and   hh_size_code in $sizeSelection
         |      and   hh_race_code in $raceSelection
         |      and   population_density_code in $populationDensitySelection
         |      and   transaction_date >= '$intabStartDate'
         |      and   transaction_date <= '$intabEndDate'
         |      and   flight_id = '$flightSelection'
         |      and   impression_flag = 1)""".stripMargin)
  }
  if (inputValue == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select distinct tb1.household_pel,
         |             tb1.projected_weight
         |      from $ctvPanel tb1
         |        join $ctvHouseholds tb2 on tb1.household_pel = tb2.household_pel
         |        join $ctvTransactions tb3 on tb1.household_pel = tb3.household_pel
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate'
         |      and   hh_age_code in $ageSelection
         |      and   hh_income_code in $incomeSelection
         |      and   hh_size_code in $sizeSelection
         |      and   hh_race_code in $raceSelection
         |      and   population_density_code in $populationDensitySelection
         |      and   dma_code in ($dmaSelection)
         |      and   transaction_date >= '$intabStartDate'
         |      and   transaction_date <= '$intabEndDate'
         |      and   flight_id = '$flightSelection'
         |      and   impression_flag = 1)""".stripMargin)
  }
  outputDF
}

def householdImpressions(inputValue: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputValue == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.household_pel,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select household_pel,
         |                         count(flight_id) impressions
         |                  from $ctvTransactions
         |                  where transaction_date >= '$intabStartDate'
         |                  and   transaction_date <= '$intabEndDate'
         |                  and   flight_id = '$flightSelection'
         |                  and   impression_flag = 1
         |                  group by household_pel) tb1
         |              join $ctvPanel tb2 on tb1.household_pel = tb2.household_pel
         |              join $ctvHouseholds tb3 on tb1.household_pel = tb3.household_pel
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate'
         |            and   hh_age_code in $ageSelection
         |            and   hh_income_code in $incomeSelection
         |            and   hh_size_code in $sizeSelection
         |            and   hh_race_code in $raceSelection
         |            and   population_density_code in $populationDensitySelection) tb0
         |      group by impressions)""".stripMargin)
  }
  if (inputValue == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.household_pel,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select household_pel,
         |                         count(flight_id) impressions
         |                  from $ctvTransactions
         |                  where transaction_date >= '$intabStartDate'
         |                  and   transaction_date <= '$intabEndDate'
         |                  and   flight_id = '$flightSelection'
         |                  and   impression_flag = 1
         |                  group by household_pel) tb1
         |              join $ctvPanel tb2 on tb1.household_pel = tb2.household_pel
         |              join $ctvHouseholds tb3 on tb1.household_pel = tb3.household_pel
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate'
         |            and   hh_age_code in $ageSelection
         |            and   hh_income_code in $incomeSelection
         |            and   hh_size_code in $sizeSelection
         |            and   hh_race_code in $raceSelection
         |            and   population_density_code in $populationDensitySelection
         |            and   dma_code in ($dmaSelection)) tb0
         |      group by impressions)""".stripMargin)
  }
  outputDF
}


val availableHouseholdsDF: DataFrame = availableHouseholds(inputValue = localNational)
val householdsViewedDF: DataFrame = householdsViewed(inputValue = localNational)
val householdImpressionsDF: DataFrame = householdImpressions(inputValue = localNational)

val summaryDF: DataFrame = availableHouseholdsDF
  .join(householdsViewedDF, "id")
  .join(householdImpressionsDF, "id")
  .join(flightInfoDF, Seq("id"), "left")
  .withColumn("Target Name", lit(targetName))
  .withColumn("Measurement Period", lit(intabStartDate + " – " + intabEndDate))
  .withColumn("Reach (unweighted)", $"households_viewed_unweighted" / $"households_available_unweighted" * 100)
  .withColumn("Reach (weighted)", $"households_viewed_weighted" / $"households_available_weighted" * 100)
  .withColumn("Frequency (unweighted)", $"household_views_unweighted" / $"households_viewed_unweighted")
  .withColumn("Frequency (weighted)", $"household_views_weighted" / $"households_viewed_weighted")
  .withColumn("GRP (unweighted)", $"household_views_unweighted" / $"households_available_unweighted" * 100)
  .withColumn("GRP (weighted)", $"household_views_weighted" / $"households_available_weighted" * 100)
  .withColumnRenamed("advertiser", "Advertiser")
  .withColumnRenamed("campaign", "Campaign")
  .withColumnRenamed("flight", "Flight")
  .withColumnRenamed("flight_period", "Flight Period")
  .withColumnRenamed("households_available_unweighted", "Available Households (unweighted)")
  .withColumnRenamed("households_available_weighted", "Available Households (weighted)")
  .withColumnRenamed("households_viewed_unweighted", "Exposed Households (unweighted)")
  .withColumnRenamed("households_viewed_weighted", "Exposed Households (weighted)")
  .select(
    $"Advertiser",
    $"Campaign",
    $"Flight",
    $"Flight Period",
    $"Target Name",
    $"Measurement Period",
    $"Available Households (unweighted)",
    $"Available Households (weighted)",
    $"Exposed Households (unweighted)",
    $"Exposed Households (weighted)",
    $"Reach (unweighted)",
    $"Reach (weighted)",
    $"Frequency (unweighted)",
    $"Frequency (weighted)",
    $"GRP (unweighted)",
    $"GRP (weighted)")

z.show(summaryDF)