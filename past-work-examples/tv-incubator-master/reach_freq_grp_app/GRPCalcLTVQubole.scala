import org.apache.spark.sql.DataFrame

val linearCommercialPanel: String = "default.jwillingham_linear_commercial_panel_weighted_weekly_20180205_20180225"
val linearCommercialViewership: String = "default.jwillingham_linear_commercials"
val targetAudience: String = "default.jwillingham_lexus_audience"

val targetName: String = "Denver, Luxury Auto Intenders"
val timePeriod: Int = 1 // 0 = day, 1 = week, 2 = month, 3 = quarter
val intabStartDate: String = "2018-02-12"
val intabEndDate: String = "2018-02-18"
val localNational: Int = 1 // 0 = National, 1 = Local
val useAudience: Int = 1 // 0 = No, 1 = Yes
val commercialIdList: String = "'1682586','1682587','1684159','1684160','1684988','1691399','1694306','1698918','1698919','1703253','1703254'"
val dmaSelection: String = "751" // Use Nielsen DMA codes, separated by commas

def availableHouseholds(inputGeography: Int, inputAudience: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputGeography == 0 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership) tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where dma_code in ($dmaSelection)) tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 0 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership) tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb1.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_available_unweighted,
         |       sum(projected_weight) households_available_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where dma_code in ($dmaSelection)) tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb1.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  outputDF
}

def householdsViewed(inputGeography: Int, inputAudience: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputGeography == 0 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where commercial_id in ($commercialIdList)
         |              and   broadcast_date >= '$intabStartDate'
         |              and   broadcast_date <= '$intabEndDate') tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where commercial_id in ($commercialIdList)
         |              and   broadcast_date >= '$intabStartDate'
         |              and   broadcast_date <= '$intabEndDate'
         |              and   dma_code in ($dmaSelection)) tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 0 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where commercial_id in ($commercialIdList)
         |              and   broadcast_date >= '$intabStartDate'
         |              and   broadcast_date <= '$intabEndDate') tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb1.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       count(*) households_viewed_unweighted,
         |       sum(projected_weight) households_viewed_weighted
         |from (select tb1.hhid,
         |             tb1.projected_weight
         |      from $linearCommercialPanel tb1
         |        join (select distinct hhid
         |              from $linearCommercialViewership
         |              where commercial_id in ($commercialIdList)
         |              and   broadcast_date >= '$intabStartDate'
         |              and   broadcast_date <= '$intabEndDate'
         |              and   dma_code in ($dmaSelection)) tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb1.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate')""".stripMargin)
  }
  outputDF
}

def householdImpressions(inputGeography: Int, inputAudience: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputGeography == 0 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.hhid,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select hhid,
         |                         count(*) impressions
         |                  from $linearCommercialViewership
         |                  where commercial_id in ($commercialIdList)
         |                  and   broadcast_date >= '$intabStartDate'
         |                  and   broadcast_date <= '$intabEndDate'
         |                  group by hhid) tb1
         |              join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate') tb0
         |      group by impressions)""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.hhid,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select hhid,
         |                         count(*) impressions
         |                  from $linearCommercialViewership
         |                  where commercial_id in ($commercialIdList)
         |                  and   broadcast_date >= '$intabStartDate'
         |                  and   broadcast_date <= '$intabEndDate'
         |                  and   dma_code in ($dmaSelection)
         |                  group by hhid) tb1
         |              join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate') tb0
         |      group by impressions)""".stripMargin)
  }
  if (inputGeography == 0 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.hhid,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select hhid,
         |                         count(*) impressions
         |                  from $linearCommercialViewership
         |                  where commercial_id in ($commercialIdList)
         |                  and   broadcast_date >= '$intabStartDate'
         |                  and   broadcast_date <= '$intabEndDate'
         |                  group by hhid) tb1
         |              join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |              join $targetAudience tb3 on tb1.hhid = tb3.id
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate') tb0
         |      group by impressions)""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select 0 id,
         |       sum(impressions*households_unweighted) household_views_unweighted,
         |       sum(impressions*households_weighted) household_views_weighted
         |from (select impressions,
         |             count(*) households_unweighted,
         |             sum(projected_weight) households_weighted
         |      from (select tb1.hhid,
         |                   tb1.impressions,
         |                   tb2.projected_weight
         |            from (select hhid,
         |                         count(*) impressions
         |                  from $linearCommercialViewership
         |                  where commercial_id in ($commercialIdList)
         |                  and   broadcast_date >= '$intabStartDate'
         |                  and   broadcast_date <= '$intabEndDate'
         |                  and   dma_code in ($dmaSelection)
         |                  group by hhid) tb1
         |              join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |              join $targetAudience tb3 on tb1.hhid = tb3.id
         |            where time_period = $timePeriod
         |            and   intab_start_date = '$intabStartDate') tb0
         |      group by impressions)""".stripMargin)
  }
  outputDF
}

val availableHouseholdsDF: DataFrame = availableHouseholds(inputGeography = localNational, inputAudience = useAudience)
val householdsViewedDF: DataFrame = householdsViewed(inputGeography = localNational, inputAudience = useAudience)
val householdImpressionsDF: DataFrame = householdImpressions(inputGeography = localNational, inputAudience = useAudience)

val summaryDF: DataFrame = availableHouseholdsDF
  .join(householdsViewedDF, "id")
  .join(householdImpressionsDF, "id")
  .withColumn("Target Name", lit(targetName))
  .withColumn("Measurement Period", lit(intabStartDate + " – " + intabEndDate))
  .withColumn("Percent Reach (unweighted)", $"households_viewed_unweighted" / $"households_available_unweighted" * 100)
  .withColumn("Percent Reach (weighted)", $"households_viewed_weighted" / $"households_available_weighted" * 100)
  .withColumn("Average Frequency (unweighted)", $"household_views_unweighted" / $"households_viewed_unweighted")
  .withColumn("Average Frequency (weighted)", $"household_views_weighted" / $"households_viewed_weighted")
  .withColumn("GRP (unweighted)", $"household_views_unweighted" / $"households_available_unweighted" * 100)
  .withColumn("GRP (weighted)", $"household_views_weighted" / $"households_available_weighted" * 100)
  .withColumnRenamed("households_available_unweighted", "Available Households (unweighted)")
  .withColumnRenamed("households_available_weighted", "Available Households (weighted)")
  .withColumnRenamed("households_viewed_unweighted", "Exposed Households (unweighted)")
  .withColumnRenamed("households_viewed_weighted", "Exposed Households (weighted)")
  .select(
    $"Target Name",
    $"Measurement Period",
    $"Available Households (unweighted)",
    $"Available Households (weighted)",
    $"Exposed Households (unweighted)",
    $"Exposed Households (weighted)",
    $"Percent Reach (unweighted)",
    $"Percent Reach (weighted)",
    $"Average Frequency (unweighted)",
    $"Average Frequency (weighted)",
    $"GRP (unweighted)",
    $"GRP (weighted)")

z.show(summaryDF)