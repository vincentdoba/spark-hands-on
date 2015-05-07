package psug.hands.on.solutions.exercise07

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Apply a Linear Regression model trained using 500 cities picked randomly among the list of cities having more than
 * 2000 inhabitants in France on the rest of the cities having more than 2000 inhabitants in order to determine which
 * cities have more than 5000 inhabitants.
 *
 * Display the precision of the algorithm (number of good guess over total number of cities) and ten cities that
 * were mislabeled
 *
 * file : normalized_features.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise07.MachineLearning"
 */
object MachineLearning extends App with SparkContextInitiator {

  val inputFile = "data/normalized_features.json"

  val sparkContext = initContext("machineLearning")
  val sqlContext = new SQLContext(sparkContext)

  import org.apache.spark.sql.functions._

  val toVector = udf[org.apache.spark.mllib.linalg.Vector, Seq[Double]](seq => Vectors.dense(seq.toArray))

  val normalizedFeatures = sqlContext.jsonFile(inputFile)
  normalizedFeatures.registerTempTable("dataset")

  val trainingData:DataFrame = normalizedFeatures.sample(false, 0.1)
  trainingData.registerTempTable("training")

  val testData = sqlContext.sql("SELECT name, features FROM dataset EXCEPT SELECT name, features FROM training")

  val logisticRegression = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
    .setFeaturesCol("features")
    .setLabelCol("category")

  val pipeline = new Pipeline()
    .setStages(Array(logisticRegression))

  val model = pipeline.fit(trainingData.select(trainingData("name"), toVector(trainingData("features")).as("features"), trainingData("category")))

  val predictions = model.transform(testData.select(testData("name"), toVector(testData("features")).as("features")))

  predictions.registerTempTable("predictions")

  val results = sqlContext
    .sql("SELECT predictions.name, category, prediction FROM predictions JOIN dataset ON dataset.name = predictions.name")
    .cache()

  val resultStats: MachineLearningStats = results
    .map(extractStats)
    .reduce((a, b) => a.aggregate(b))

  val misLabeledCitiesExamples = results
    .filter(results("category") !== results("prediction"))
    .select("name", "category", "prediction")
    .map(stringifyResultRow)
    .take(10)

  def extractStats(row: Row) = {
    val total = 1
    val correctlyLabeled = if (row.getDouble(1) == row.getDouble(2)) 1 else 0
    val positive = if (row.getDouble(1) == 1) 1 else 0
    val positiveCorrectlyLabeled = if (row.getDouble(1) == 1 && row.getDouble(2) == 1) 1 else 0
    val positiveLabeled = if (row.getDouble(2) == 1) 1 else 0
    MachineLearningStats(total, correctlyLabeled, positive, positiveCorrectlyLabeled, positiveLabeled)
  }

  def stringifyResultRow(row:Row) = (row.getDouble(1), row.getDouble(2)) match {
    case (1, 0) => row.getString(0) + " (actual >5000, labeled <5000)"
    case (0, 1) => row.getString(0) + " (actual <5000, labeled >5000)"
    case _ => "error, " + row.getString(0) + " is well labeled"
  }

  val accuracy:Long = resultStats.correctlyLabeled * 100 / resultStats.total
  val precision:Long = resultStats.positiveCorrectlyLabeled * 100 / resultStats.positive
  val recall:Long = resultStats.positiveCorrectlyLabeled * 100 / resultStats.positiveLabeled

  println("Total well labeled cities percentage is " + accuracy  + "%")
  println("Precision percentage for >5000 cities category is " + precision  + "%")
  println("Recall percentage for >5000 cities category is " + recall  + "%")
  println("F-Score for >5000 cities is " + (2*recall*precision/(precision+recall)) + " %")
  println("Examples of cities mislabeled : " + misLabeledCitiesExamples.mkString(", "))

  sparkContext.stop()

}

case class MachineLearningStats(total: Int, correctlyLabeled: Int, positive: Int, positiveCorrectlyLabeled: Int, positiveLabeled:Int) {

  def aggregate(that: MachineLearningStats) = MachineLearningStats(
    this.total + that.total, 
    this.correctlyLabeled + that.correctlyLabeled,
    this.positive + that.positive, 
    this.positiveCorrectlyLabeled + that.positiveCorrectlyLabeled,
    this.positiveLabeled + that.positiveLabeled
  )

}

