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
  val positiveMislabeled = total.where(total("category") === 1 && total("prediction") === 0).first().getLong(2)
  val negativeMislabeled = total.where(total("category") === 0 && total("prediction") === 1).first().getLong(2)

  val positiveRecall = positiveWellLabeled*100/(positiveWellLabeled + positiveMislabeled)
  val positivePrecision = positiveWellLabeled*100/(positiveWellLabeled + negativeMislabeled)

  val negativeRecall = negativeWellLabeled*100/(negativeWellLabeled + negativeMislabeled)
  val negativePrecision = negativeWellLabeled*100/(negativeWellLabeled + positiveMislabeled)

  val accuracy = (positiveWellLabeled + negativeWellLabeled)*100/(positiveMislabeled + positiveWellLabeled + negativeMislabeled + negativeWellLabeled)
  
  val mislabeledExamples = result.where(result("category") !== result("prediction"))
    .map(row => (row.getString(0), row.getDouble(1) == 1))
    .take(10)

  val wellLabeledExamples = result.where(result("category") === result("prediction"))
    .map(row => (row.getString(0), row.getDouble(1) == 1))
    .take(10)

  val maxSize = List(30, nameMaxSize(wellLabeledExamples), nameMaxSize(mislabeledExamples)).max

  displayGeneralStats(accuracy)
  displayCategoryStats(maxSize)("0", negativePrecision, negativeRecall)
  displayCategoryStats(maxSize)("1", positivePrecision, positiveRecall)
  displayWellLabeledExamples(maxSize)(wellLabeledExamples)
  displayMislabeledExamples(maxSize)(mislabeledExamples)


  def displayCategoryStats(maxSize:Int)(name: String, precision: Long, recall: Long) {

    val fScore = 2*recall*precision / (recall + precision)
    def fixedAlignLabel = alignLabel(maxSize)_

    println(s"======= For Category $name =======")
    println(line(maxSize))
    val precisionLabel = fixedAlignLabel("Precision")
    println(s"- $precisionLabel : $precision %")
    val recallLabel = fixedAlignLabel("Recall")
    println(s"- $recallLabel : $recall %")
    val F1ScoreLabel = fixedAlignLabel("F1-Score")
    println(s"- $F1ScoreLabel : $fScore")
    println(line(maxSize))
  }

  def displayGeneralStats(accuracy:Long) {
    println(line(maxSize))
    println("======== General Stats =======")
    println(line(maxSize))
    println(s"- accuracy              : $accuracy %")
    println(line(maxSize))
  }

  def displayWellLabeledExamples(maxSize:Int)(cities: Array[(String, Boolean)]) {
    println("===== Well Labeled Cities ====")
    println(line(maxSize))
    cities.foreach{
      case(name,true) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : more than 5000 inhabitants")
      case(name,false) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : less than 5000 inhabitants")
    }
    println(line(maxSize))
  }

  def displayMislabeledExamples(maxSize:Int)(cities: Array[(String, Boolean)]) {
    println("====== Mislabeled Cities =====")
    println(line(maxSize))
    cities.foreach{
      case(name,true) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : actually more than 5000 inhabitants")
      case(name,false) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : actually less than 5000 inhabitants")
    }
    println(line(maxSize))
  }
  
  def line(maxSize:Int) = List.fill(maxSize)("-").mkString("")

  def alignLabel(maxSize:Int)(element:String) = element + List.fill(maxSize - element.length)(" ").mkString("")

  def nameMaxSize(cities:Array[(String, Boolean)]):Int = cities.map(_._1.length).max
}
