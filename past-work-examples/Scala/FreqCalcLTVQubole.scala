import org.apache.spark.sql.DataFrame

val linearCommercialPanel: String = "default.jwillingham_linear_commercial_panel_weighted_weekly_20180205_20180225"
val linearCommercialViewership: String = "default.jwillingham_linear_commercials"
val targetAudience: String = "default.jwillingham_lexus_audience"

val brandSelection: String = "Lexus"
val targetName: String = "New York, Luxury Auto Intenders"
val timePeriod: Int = 1 // 0 = day, 1 = week, 2 = month, 3 = quarter
val intabStartDate: String = "2018-02-12"
val intabEndDate: String = "2018-02-18"
val localNational: Int = 1 // 0 = National, 1 = Local
val useAudience: Int = 1 // 0 = No, 1 = Yes
val dmaSelection: String = "501"

def householdImpressions(inputGeography: Int, inputAudience: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputGeography == 0 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select impressions,
         |       count(*) households_unweighted,
         |       sum(projected_weight) households_weighted
         |from (select tb1.hhid,
         |             tb1.impressions,
         |             tb2.projected_weight
         |      from (select hhid,
         |                   count(*) impressions
         |            from $linearCommercialViewership
         |            where commercial_brand = '$brandSelection'
         |            and   broadcast_date >= '$intabStartDate'
         |            and   broadcast_date <= '$intabEndDate'
         |            group by hhid) tb1
         |        join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate') tb0
         |group by impressions""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select impressions,
         |       count(*) households_unweighted,
         |       sum(projected_weight) households_weighted
         |from (select tb1.hhid,
         |             tb1.impressions,
         |             tb2.projected_weight
         |      from (select hhid,
         |                   count(*) impressions
         |            from $linearCommercialViewership
         |            where commercial_brand = '$brandSelection'
         |            and   broadcast_date >= '$intabStartDate'
         |            and   broadcast_date <= '$intabEndDate'
         |            and   dma_code in ($dmaSelection)
         |            group by hhid) tb1
         |        join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate') tb0
         |group by impressions""".stripMargin)
  }
  if (inputGeography == 0 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select impressions,
         |       count(*) households_unweighted,
         |       sum(projected_weight) households_weighted
         |from (select tb1.hhid,
         |             tb1.impressions,
         |             tb2.projected_weight
         |      from (select hhid,
         |                   count(*) impressions
         |            from $linearCommercialViewership
         |            where commercial_brand = '$brandSelection'
         |            and   broadcast_date >= '$intabStartDate'
         |            and   broadcast_date <= '$intabEndDate'
         |            group by hhid) tb1
         |        join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb2.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate') tb0
         |group by impressions""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select impressions,
         |       count(*) households_unweighted,
         |       sum(projected_weight) households_weighted
         |from (select tb1.hhid,
         |             tb1.impressions,
         |             tb2.projected_weight
         |      from (select hhid,
         |                   count(*) impressions
         |            from $linearCommercialViewership
         |            where commercial_brand = '$brandSelection'
         |            and   broadcast_date >= '$intabStartDate'
         |            and   broadcast_date <= '$intabEndDate'
         |            and   dma_code in ($dmaSelection)
         |            group by hhid) tb1
         |        join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |        join $targetAudience tb3 on tb2.hhid = tb3.id
         |      where time_period = $timePeriod
         |      and   intab_start_date = '$intabStartDate') tb0
         |group by impressions""".stripMargin)
  }
  outputDF
}

val householdImpressionsDF: DataFrame = householdImpressions(inputGeography = localNational, inputAudience = useAudience)

val summaryDF: DataFrame = householdImpressionsDF
  .withColumn("Target Name", lit(targetName))
  .withColumn("Brand", lit(brandSelection))
  .withColumn("Measurement Period", lit(intabStartDate + " – " + intabEndDate))
  .withColumnRenamed("impressions", "Impressions")
  .withColumnRenamed("households_unweighted", "Households (unweighted)")
  .withColumnRenamed("households_weighted", "Households (weighted)")
  .select(
    $"Target Name",
    $"Brand",
    $"Measurement Period",
    $"Impressions",
    $"Households (unweighted)",
    $"Households (weighted)")

z.show(summaryDF)