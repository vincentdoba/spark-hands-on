package psug.hands.on.exercise08

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Compute statistics on result of labeling test_cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise08.DisplayResult"
 *
 */
object DisplayResult extends App {

  val inputFile = "data/labeled_cities.json"

  val conf = new SparkConf().setMaster("local").setAppName("machineLearningResultDisplayer")
  val sparkContext = new SparkContext(conf)
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
  val lineSize = maxSize + 40

  displayGeneralStats(accuracy)
  displayCategoryStats("0", negativePrecision, negativeRecall)
  displayCategoryStats("1", positivePrecision, positiveRecall)
  displayWellLabeledExamples(wellLabeledExamples)
  displayMislabeledExamples(mislabeledExamples)


  def displayCategoryStats(name: String, precision: Long, recall: Long) {

    val fScore = 2*recall*precision / (recall + precision)
    def fixedAlignLabel = alignLabel(maxSize)_

    println(titleBox(s"For Category $name"))
    println(line(lineSize))
    val precisionLabel = fixedAlignLabel("Precision")
    println(s"- $precisionLabel : $precision %")
    val recallLabel = fixedAlignLabel("Recall")
    println(s"- $recallLabel : $recall %")
    val F1ScoreLabel = fixedAlignLabel("F1-Score")
    println(s"- $F1ScoreLabel : $fScore")
    println(line(lineSize))
  }

  def displayGeneralStats(accuracy:Long) {
    println(line(lineSize))
    println(titleBox("General Stats"))
    println(line(lineSize))
    val label = alignLabel(maxSize)("Accuracy")
    println(s"- $label : $accuracy %")
    println(line(lineSize))
  }

  def displayWellLabeledExamples(cities: Array[(String, Boolean)]) {
    println(titleBox("Well Labeled Cities"))
    println(line(lineSize))
    cities.foreach{
      case(name,true) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : more than 5000 inhabitants")
      case(name,false) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : less than 5000 inhabitants")
    }
    println(line(lineSize))
  }

  def displayMislabeledExamples(cities: Array[(String, Boolean)]) {
    println(titleBox("Mislabeled Cities"))
    println(line(lineSize))
    cities.foreach{
      case(name,true) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : actually more than 5000 inhabitants")
      case(name,false) =>
        val label = alignLabel(maxSize)(name)
        println(s"- $label : actually less than 5000 inhabitants")
    }
    println(line(lineSize))
  }

  def titleBox(title:String) = {
    val remainingCharacters: Int = lineSize - title.length - 2
    val rightFill = List.fill(remainingCharacters/2)("=").mkString("")
    val leftFill = List.fill(remainingCharacters/2 + (remainingCharacters % 2))("=").mkString("")
    s"$leftFill $title $rightFill"
  }
  
  def line(maxSize:Int) = List.fill(maxSize)("-").mkString("")

  def alignLabel(maxSize:Int)(element:String) = element + List.fill(maxSize - element.length)(" ").mkString("")

  def nameMaxSize(cities:Array[(String, Boolean)]):Int = cities.map(_._1.length).max
}
