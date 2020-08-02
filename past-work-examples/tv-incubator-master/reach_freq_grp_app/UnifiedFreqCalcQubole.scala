import org.apache.spark.sql.DataFrame

val connectedImpressions: String = "default.jwillingham_ctv_impressions"
val linearCommercialPanel: String = "default.jwillingham_linear_commercial_panel_weighted_weekly_20180205_20180225"
val linearCommercialViewership: String = "default.jwillingham_linear_commercials"
val targetAudience: String = "default.jwillingham_lexus_audience"

val brandSelection: String = "Lexus"
val targetName: String = "National, All Households"
val timePeriod: Int = 1 // 0 = day, 1 = week, 2 = month, 3 = quarter
val intabStartDate: String = "2018-02-12"
val intabEndDate: String = "2018-02-18"
val localNational: Int = 0 // 0 = National, 1 = Local
val useAudience: Int = 0 // 0 = No, 1 = Yes
val dmaSelection: String = "501"

def householdImpressions(inputGeography: Int, inputAudience: Int): DataFrame = {
  var outputDF: DataFrame = spark.sqlContext.emptyDataFrame
  if (inputGeography == 0 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select coalesce(linear.impressions,connected.impressions) impressions,
         |       case
         |         when ltv_households_unweighted is null then 0
         |         else ltv_households_unweighted
         |       end ltv_households_unweighted,
         |       case
         |         when ltv_households_weighted is null then 0
         |         else ltv_households_weighted
         |       end ltv_households_weighted,
         |       case
         |         when ctv_households_unweighted is null then 0
         |         else ctv_households_unweighted
         |       end ctv_households_unweighted,
         |       case
         |         when ctv_households_weighted is null then 0
         |         else ctv_households_weighted
         |       end ctv_households_weighted
         |from (select ltv_impressions impressions,
         |             count(*) ltv_households_unweighted,
         |             sum(projected_weight) ltv_households_weighted
         |      from (select ltv.hhid,
         |                   ltv.projected_weight,
         |                   ltv.impressions ltv_impressions
         |            from (select tb1.hhid,
         |                         tb1.impressions,
         |                         tb2.projected_weight
         |                  from (select hhid,
         |                               count(*) impressions
         |                        from $linearCommercialViewership
         |                        where commercial_brand = '$brandSelection'
         |                        and   broadcast_date >= '$intabStartDate'
         |                        and   broadcast_date <= '$intabEndDate'
         |                        group by hhid) tb1
         |                    join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                  where time_period = $timePeriod
         |                  and   intab_start_date = '$intabStartDate') ltv
         |              join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |      group by ltv_impressions) linear
         |  full join (select ctv_impressions impressions,
         |                    count(hhid) ctv_households_unweighted,
         |                    sum(projected_weight) ctv_households_weighted
         |             from (select ltv.hhid,
         |                          ltv.projected_weight,
         |                          ctv.impressions ctv_impressions
         |                   from (select tb1.hhid,
         |                                tb1.impressions,
         |                                tb2.projected_weight
         |                         from (select hhid,
         |                                      count(*) impressions
         |                               from $linearCommercialViewership
         |                               where commercial_brand = '$brandSelection'
         |                               and   broadcast_date >= '$intabStartDate'
         |                               and   broadcast_date <= '$intabEndDate'
         |                               group by hhid) tb1
         |                           join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                         where time_period = $timePeriod
         |                         and   intab_start_date = '$intabStartDate') ltv
         |                     join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |             group by ctv_impressions) connected on linear.impressions = connected.impressions""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 0) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select coalesce(linear.impressions,connected.impressions) impressions,
         |       case
         |         when ltv_households_unweighted is null then 0
         |         else ltv_households_unweighted
         |       end ltv_households_unweighted,
         |       case
         |         when ltv_households_weighted is null then 0
         |         else ltv_households_weighted
         |       end ltv_households_weighted,
         |       case
         |         when ctv_households_unweighted is null then 0
         |         else ctv_households_unweighted
         |       end ctv_households_unweighted,
         |       case
         |         when ctv_households_weighted is null then 0
         |         else ctv_households_weighted
         |       end ctv_households_weighted
         |from (select ltv_impressions impressions,
         |             count(*) ltv_households_unweighted,
         |             sum(projected_weight) ltv_households_weighted
         |      from (select ltv.hhid,
         |                   ltv.projected_weight,
         |                   ltv.impressions ltv_impressions
         |            from (select tb1.hhid,
         |                         tb1.impressions,
         |                         tb2.projected_weight
         |                  from (select hhid,
         |                               count(*) impressions
         |                        from $linearCommercialViewership
         |                        where commercial_brand = '$brandSelection'
         |                        and   broadcast_date >= '$intabStartDate'
         |                        and   broadcast_date <= '$intabEndDate'
         |                        and   dma_code in ($dmaSelection)
         |                        group by hhid) tb1
         |                    join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                  where time_period = $timePeriod
         |                  and   intab_start_date = '$intabStartDate') ltv
         |              join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |      group by ltv_impressions) linear
         |  full join (select ctv_impressions impressions,
         |                    count(hhid) ctv_households_unweighted,
         |                    sum(projected_weight) ctv_households_weighted
         |             from (select ltv.hhid,
         |                          ltv.projected_weight,
         |                          ctv.impressions ctv_impressions
         |                   from (select tb1.hhid,
         |                                tb1.impressions,
         |                                tb2.projected_weight
         |                         from (select hhid,
         |                                      count(*) impressions
         |                               from $linearCommercialViewership
         |                               where commercial_brand = '$brandSelection'
         |                               and   broadcast_date >= '$intabStartDate'
         |                               and   broadcast_date <= '$intabEndDate'
         |                               and   dma_code in ($dmaSelection)
         |                               group by hhid) tb1
         |                           join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                         where time_period = $timePeriod
         |                         and   intab_start_date = '$intabStartDate') ltv
         |                     join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |             group by ctv_impressions) connected on linear.impressions = connected.impressions""".stripMargin)
  }
  if (inputGeography == 0 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select coalesce(linear.impressions,connected.impressions) impressions,
         |       case
         |         when ltv_households_unweighted is null then 0
         |         else ltv_households_unweighted
         |       end ltv_households_unweighted,
         |       case
         |         when ltv_households_weighted is null then 0
         |         else ltv_households_weighted
         |       end ltv_households_weighted,
         |       case
         |         when ctv_households_unweighted is null then 0
         |         else ctv_households_unweighted
         |       end ctv_households_unweighted,
         |       case
         |         when ctv_households_weighted is null then 0
         |         else ctv_households_weighted
         |       end ctv_households_weighted
         |from (select ltv_impressions impressions,
         |             count(*) ltv_households_unweighted,
         |             sum(projected_weight) ltv_households_weighted
         |      from (select ltv.hhid,
         |                   ltv.projected_weight,
         |                   ltv.impressions ltv_impressions
         |            from (select tb1.hhid,
         |                         tb1.impressions,
         |                         tb2.projected_weight
         |                  from (select hhid,
         |                               count(*) impressions
         |                        from $linearCommercialViewership
         |                        where commercial_brand = '$brandSelection'
         |                        and   broadcast_date >= '$intabStartDate'
         |                        and   broadcast_date <= '$intabEndDate'
         |                        group by hhid) tb1
         |                    join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                    join $targetAudience tb3 on tb2.hhid = tb3.id
         |                  where time_period = $timePeriod
         |                  and   intab_start_date = '$intabStartDate') ltv
         |              join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |      group by ltv_impressions) linear
         |  full join (select ctv_impressions impressions,
         |                    count(hhid) ctv_households_unweighted,
         |                    sum(projected_weight) ctv_households_weighted
         |             from (select ltv.hhid,
         |                          ltv.projected_weight,
         |                          ctv.impressions ctv_impressions
         |                   from (select tb1.hhid,
         |                                tb1.impressions,
         |                                tb2.projected_weight
         |                         from (select hhid,
         |                                      count(*) impressions
         |                               from $linearCommercialViewership
         |                               where commercial_brand = '$brandSelection'
         |                               and   broadcast_date >= '$intabStartDate'
         |                               and   broadcast_date <= '$intabEndDate'
         |                               group by hhid) tb1
         |                           join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                           join $targetAudience tb3 on tb2.hhid = tb3.id
         |                         where time_period = $timePeriod
         |                         and   intab_start_date = '$intabStartDate') ltv
         |                     join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |             group by ctv_impressions) connected on linear.impressions = connected.impressions""".stripMargin)
  }
  if (inputGeography == 1 && inputAudience == 1) {
    outputDF = spark.sqlContext.sql(
      s"""
         |select coalesce(linear.impressions,connected.impressions) impressions,
         |       case
         |         when ltv_households_unweighted is null then 0
         |         else ltv_households_unweighted
         |       end ltv_households_unweighted,
         |       case
         |         when ltv_households_weighted is null then 0
         |         else ltv_households_weighted
         |       end ltv_households_weighted,
         |       case
         |         when ctv_households_unweighted is null then 0
         |         else ctv_households_unweighted
         |       end ctv_households_unweighted,
         |       case
         |         when ctv_households_weighted is null then 0
         |         else ctv_households_weighted
         |       end ctv_households_weighted
         |from (select ltv_impressions impressions,
         |             count(*) ltv_households_unweighted,
         |             sum(projected_weight) ltv_households_weighted
         |      from (select ltv.hhid,
         |                   ltv.projected_weight,
         |                   ltv.impressions ltv_impressions
         |            from (select tb1.hhid,
         |                         tb1.impressions,
         |                         tb2.projected_weight
         |                  from (select hhid,
         |                               count(*) impressions
         |                        from $linearCommercialViewership
         |                        where commercial_brand = '$brandSelection'
         |                        and   broadcast_date >= '$intabStartDate'
         |                        and   broadcast_date <= '$intabEndDate'
         |                        and   dma_code in ($dmaSelection)
         |                        group by hhid) tb1
         |                    join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                    join $targetAudience tb3 on tb2.hhid = tb3.id
         |                  where time_period = $timePeriod
         |                  and   intab_start_date = '$intabStartDate') ltv
         |              join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |      group by ltv_impressions) linear
         |  full join (select ctv_impressions impressions,
         |                    count(hhid) ctv_households_unweighted,
         |                    sum(projected_weight) ctv_households_weighted
         |             from (select ltv.hhid,
         |                          ltv.projected_weight,
         |                          ctv.impressions ctv_impressions
         |                   from (select tb1.hhid,
         |                                tb1.impressions,
         |                                tb2.projected_weight
         |                         from (select hhid,
         |                                      count(*) impressions
         |                               from $linearCommercialViewership
         |                               where commercial_brand = '$brandSelection'
         |                               and   broadcast_date >= '$intabStartDate'
         |                               and   broadcast_date <= '$intabEndDate'
         |                               and   dma_code in ($dmaSelection)
         |                               group by hhid) tb1
         |                           join $linearCommercialPanel tb2 on tb1.hhid = tb2.hhid
         |                           join $targetAudience tb3 on tb2.hhid = tb3.id
         |                         where time_period = $timePeriod
         |                         and   intab_start_date = '$intabStartDate') ltv
         |                     join $connectedImpressions ctv on ltv.hhid = ctv.hhid) tb0
         |             group by ctv_impressions) connected on linear.impressions = connected.impressions""".stripMargin)
  }
  outputDF
}

val householdImpressionsDF: DataFrame = householdImpressions(inputGeography = localNational, inputAudience = useAudience)

val summaryDF: DataFrame = householdImpressionsDF
  .withColumn("Target Name", lit(targetName))
  .withColumn("Brand", lit(brandSelection))
  .withColumn("Measurement Period", lit(intabStartDate + " – " + intabEndDate))
  .withColumnRenamed("impressions", "Impressions")
  .withColumnRenamed("ltv_households_unweighted", "Linear Households (unweighted)")
  .withColumnRenamed("ltv_households_weighted", "Linear Households (weighted)")
  .withColumnRenamed("ctv_households_unweighted", "CTV Households (unweighted)")
  .withColumnRenamed("ctv_households_weighted", "CTV Households (weighted)")
  .select(
    $"Target Name",
    $"Brand",
    $"Measurement Period",
    $"Impressions",
    $"Linear Households (unweighted)",
    $"Linear Households (weighted)",
    $"CTV Households (unweighted)",
    $"CTV Households (weighted)")

z.show(summaryDF)