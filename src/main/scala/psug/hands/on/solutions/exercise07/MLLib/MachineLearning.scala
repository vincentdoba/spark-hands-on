package psug.hands.on.solutions.exercise07.MLLib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}
import psug.hands.on.solutions.SparkContextInitiator
import psug.hands.on.solutions.exercise07.{MLHelpers, MachineLearningStats}

/**
 * Apply a Linear Regression model trained using 500 cities picked randomly among the list of cities having more than
 * 2000 inhabitants in France on the rest of the cities having more than 2000 inhabitants in order to determine which
 * cities have more than 5000 inhabitants.
 *
 * Display the precision of the algorithm (number of good guess over total number of cities) and ten cities that
 * were mislabeled
 *
 * file : normalized_cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise07.MLLib.MachineLearning"
 */
object MachineLearning extends App with SparkContextInitiator with MLHelpers {

  val inputFile = "data/normalized_cities.json"

  val sparkContext = initContext("machineLearningMLLib")
  val sqlContext = new SQLContext(sparkContext)

  import org.apache.spark.sql.functions._

  val toVector = udf[org.apache.spark.mllib.linalg.Vector, Seq[Double]](seq => Vectors.dense(seq.toArray))

  val normalizedFeatures = sqlContext.jsonFile(inputFile)
  normalizedFeatures.registerTempTable("dataset")

  val trainingData: DataFrame = normalizedFeatures.sample(false, 0.1).select("name", "category", "features")
  trainingData.registerTempTable("training")

  val testData = sqlContext.sql("SELECT name, category, features FROM dataset EXCEPT SELECT name, category, features FROM training")

  val model = LogisticRegressionWithSGD.train(trainingData.map(row => LabeledPoint(row.getDouble(1), Vectors.dense(row.getSeq[Double](2).toArray))), 500, 0.01)

  val results = testData.map(row => {
    val prediction = model.predict(Vectors.dense(row.getSeq[Double](2).toArray))
    (row.getString(0), row.getDouble(1), prediction)
  }).cache()

  val resultStats: MachineLearningStats = results
    .map(extractStats)
    .reduce((a, b) => a.aggregate(b))

  val misLabeledCitiesExamples = results
    .filter(a => a._2 != a._3)
    .map(stringifyResultRow)
    .take(10)

  displayResults(resultStats, misLabeledCitiesExamples)

  sparkContext.stop()

}


