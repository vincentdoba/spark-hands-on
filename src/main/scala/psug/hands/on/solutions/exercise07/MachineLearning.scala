package psug.hands.on.solutions.exercise07

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import psug.hands.on.solutions.SparkContextInitiator

object MachineLearning extends App with SparkContextInitiator {

  val normalizedFeaturesFile = "data/normalized_features.json"

  val sparkContext = initContext("machineLearning")
  val sqlContext = new SQLContext(sparkContext)

  import org.apache.spark.sql.functions._

  val toVector = udf[org.apache.spark.mllib.linalg.Vector, Seq[Double]](seq => Vectors.dense(seq.toArray))

  val normalizedFeatures = sqlContext.jsonFile(normalizedFeaturesFile)
  normalizedFeatures.registerTempTable("dataset")

  val trainingData = normalizedFeatures.sample(false, 0.1)
  trainingData.registerTempTable("training")

  val testData = sqlContext.sql("SELECT name, features FROM dataset EXCEPT SELECT name, features FROM training")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
    .setFeaturesCol("features")
    .setLabelCol("category")

  val pipeline = new Pipeline()
    .setStages(Array(lr))

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
    val right = if (row.getDouble(1) == row.getDouble(2)) 1 else 0
    val wrong = 1 - right
    MachineLearningStats(total, right, wrong)
  }

  def stringifyResultRow(row:Row) = (row.getDouble(1), row.getDouble(2)) match {
    case (1, 0) => row.getString(0) + " (actual >5000, labeled <5000)"
    case (0, 1) => row.getString(0) + " (actual <5000, labeled >5000)"
    case _ => "error, " + row.getString(0) + " is well labeled"
  }

  println("Accuracy is " + (resultStats.right * 100 / resultStats.total) + "%")
  println("Examples of cities mislabeled : " + misLabeledCitiesExamples.mkString(", "))

  sparkContext.stop()

}

case class MachineLearningStats(total: Int, right: Int, wrong: Int) {

  def aggregate(that: MachineLearningStats) = MachineLearningStats(this.total + that.total, this.right + that.right, this.wrong + that.wrong)

}

