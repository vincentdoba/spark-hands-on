package psug.hands.on.exercise08

import org.apache.spark.sql.{DataFrame, SQLContext}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Compute statistics on result of labeling test_cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise08.DisplayResult"
 *
 */
object DisplayResult extends App with SparkContextInitiator {

  val inputFile = "data/labeled_cities.json"

  val sparkContext = initContext("machineLearningResultDisplayer")
  val sqlContext = new SQLContext(sparkContext)

  val result: DataFrame = sqlContext
    .jsonFile(inputFile)
    .select("name", "category", "prediction")
    .cache()

  import org.apache.spark.sql.functions._

  val total = result
    .groupBy("category", "prediction")
    .agg(result("category"), result("prediction"), count("name").as("number"))
    .cache()

  val positiveWellLabeled = total.where(total("category") === 1 && total("prediction") === 1).first().getLong(2)
  val negativeWellLabeled = total.where(total("category") === 0 && total("prediction") === 0).first().getLong(2)
  val positiveBadlyLabeled = total.where(total("category") === 1 && total("prediction") === 0).first().getLong(2)
  val negativeBadlyLabeled = total.where(total("category") === 0 && total("prediction") === 1).first().getLong(2)

  val positiveRecall = positiveWellLabeled*100/(positiveWellLabeled + positiveBadlyLabeled)
  val positivePrecision = positiveWellLabeled*100/(positiveWellLabeled + negativeBadlyLabeled)

  val negativeRecall = negativeWellLabeled*100/(negativeWellLabeled + negativeBadlyLabeled)
  val negativePrecision = negativeWellLabeled*100/(negativeWellLabeled + positiveBadlyLabeled)

  val accuracy = (positiveWellLabeled + negativeWellLabeled)*100/(positiveBadlyLabeled + positiveWellLabeled + negativeBadlyLabeled + negativeWellLabeled)

  displayGeneralStats(accuracy)
  displayCategoryStats("0", negativePrecision, negativeRecall)
  displayCategoryStats("1", positivePrecision, positiveRecall)


  def displayCategoryStats(name: String, precision: Long, recall: Long) {

    val fScore = 2*recall*precision / (recall + precision)

    println(s"===== For Category $name =====")
    println("--------------------------")
    println(s"- Precision         : $precision %")
    println(s"- Recall            : $recall %")
    println(s"- F1-Score          : $fScore")
    println("--------------------------")
  }

  def displayGeneralStats(accuracy:Long) {
    println("--------------------------")
    println("====== General Stats =====")
    println("--------------------------")
    println(s"- accuracy          : $accuracy %")
    println("--------------------------")
  }
}
