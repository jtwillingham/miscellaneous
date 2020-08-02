import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.parallel.mutable.ParArray
import scala.util.control.Breaks._

spark.sparkContext.setCheckpointDir("hdfs:///tmp")

val epsilon: Double = 1
val iterations: Int = 25
val projection: Int = 119600000
val dimensionArray: Array[String] = Array("dma_code")
val timePeriod: Int = 1
val viewershipType: String = "program"
val intabStartDate: ParArray[String] = ParArray("2018-02-05", "2018-02-12", "2018-02-19")
val inputSample: String = "default.jwillingham_linear_panel_unweighted"
val inputMargins: String = "default.jwillingham_demographic_populations"
val outputSample: String = "default.jwillingham_linear_program_panel_weighted_weekly_20180205_20180225"
val elementCount: AtomicInteger = new AtomicInteger(0)

def translatePeriod(inputPeriod: Int): String = {
  var mutablePeriod: String = ""
  if (inputPeriod == 0) {
    mutablePeriod = "daily"
  }
  else if (inputPeriod == 1) {
    mutablePeriod = "weekly"
  }
  else if (inputPeriod == 2) {
    mutablePeriod = "monthly"
  }
  else if (inputPeriod == 3) {
    mutablePeriod = "quarterly"
  } else {
    mutablePeriod = "unknown"
  }
  mutablePeriod
}

def sampleSelection(periodSelection: Int, startDate: String): DataFrame = {
  spark.sqlContext.sql(s"""select * from $inputSample""").filter($"time_period" === periodSelection && $"intab_start_date" === startDate && $"viewership_type" === viewershipType)
}

def rakingAlgorithm(dimensionName: String, columnNames: Array[String], sampleDF: DataFrame, populationDF: DataFrame, sampleN: Long): DataFrame = {
  val selection: Array[String] = Array("hhid") ++ columnNames ++ Array("weight")
  val populationMargins: DataFrame = populationDF
    .filter(concat($"demo_name", lit("_code")) === dimensionName)
    .groupBy($"demo_name")
    .sum("population")
    .select($"demo_name", $"sum(population)".alias("total"))
    .join(populationDF, "demo_name")
    .withColumn("proportion", $"population" / $"total")
    .select($"level_code", $"proportion")
    .withColumnRenamed("level_code", dimensionName)
  sampleDF
    .groupBy(dimensionName)
    .sum("weight")
    .withColumnRenamed("sum(weight)", "sample_margin")
    .join(sampleDF, dimensionName)
    .join(populationMargins, dimensionName)
    .withColumn("estimated_margin", lit(sampleN) * $"proportion")
    .withColumn("weight", $"weight" * $"estimated_margin" / $"sample_margin")
    .select(selection.head, selection.tail: _*)
    .checkpoint(false)
}

def maximumAbsoluteDifference(previousDF: DataFrame, currentDF: DataFrame): Double = {
  previousDF
    .select($"hhid", $"previous_weight")
    .join(currentDF, "hhid")
    .withColumn("weight_delta", abs($"previous_weight" - $"weight"))
    .agg(max("weight_delta")).first().getDouble(0)
}

intabStartDate.foreach { arrayDate: String =>
  val sampleInput: DataFrame = sampleSelection(periodSelection = timePeriod, startDate = arrayDate)
  val populationDemographics: DataFrame = spark.sqlContext.sql(s"""select * from $inputMargins""")
  val sampleSize: Long = sampleInput.count()
  val broadcastPeriod: String = translatePeriod(inputPeriod = timePeriod)
  var sampleCurrentDF: DataFrame = sampleInput.withColumn("weight", lit(1))
  breakable {
    for (loop1 <- 1 to iterations) yield {
      val samplePreviousDF: DataFrame = sampleCurrentDF.withColumnRenamed("weight", "previous_weight")
      for (loop2 <- dimensionArray.indices) yield {
        sampleCurrentDF = rakingAlgorithm(dimensionName = dimensionArray(loop2), columnNames = dimensionArray, sampleDF = sampleCurrentDF, populationDF = populationDemographics, sampleN = sampleSize)
      }
      val MAD: Double = maximumAbsoluteDifference(previousDF = samplePreviousDF, currentDF = sampleCurrentDF)
      println("Iteration " + loop1 + " for element " + arrayDate + " of the " + broadcastPeriod + " sample complete (maximum absolute difference = " + MAD + ").")
      if (MAD < epsilon) {
        println("Convergence reached after " + loop1 + " iterations, with the stopping criterion of " + MAD + " being less than " + epsilon + "!")
        println("The " + broadcastPeriod + " sample that begins on " + arrayDate + " has " + sampleSize + " records.")
        break
      }
      if (loop1 == iterations) {
        println("The maximum absolute difference in weights between iterations equals " + MAD + " after " + iterations + " iterations.")
        println("The " + broadcastPeriod + " sample that begins on " + arrayDate + " has " + sampleSize + " records.")
        break
      }
    }
  }
  val saveMode = if (elementCount.addAndGet(1) == 1) {
    SaveMode.Overwrite
  } else {
    SaveMode.Append
  }
  if (elementCount.addAndGet(1) > 2) {
    Thread.sleep(60000)
  }
  elementCount.addAndGet(1)
  sampleCurrentDF
    .withColumn("time_period", lit(timePeriod))
    .withColumn("intab_start_date", lit(arrayDate).cast(DateType))
    .withColumn("projected_weight", $"weight" * projection / sampleSize)
    .select($"hhid", $"time_period", $"intab_start_date", $"weight", $"projected_weight")
    .write.format("orc").mode(saveMode).saveAsTable(outputSample)
}