package psug.hands.on.solutions.exercise08.MLLib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import psug.hands.on.exercise05.DataSaver
import psug.hands.on.solutions.SparkContextInitiator

/**
 * - Train a Linear Regression model using training_cities.json
 * - Predict for each city in test_cities.json if it has more than 5000 inhabitants
 * - Save result in a labeled_cities.json file
 *
 * input file 1 : data/training_cities.json
 * input file 2 : data/test_cities.json
 * output file : data/labeled_cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise08.MLLib.MachineLearning"
 */
object MachineLearning extends App with SparkContextInitiator with DataSaver {

  val trainingInputFile = "data/training_cities.json"
  val testInputFile = "data/test_cities.json"
  val outputFile = "data/labeled_cities.json"

  init()

  val sparkContext = initContext("machineLearningMLLib") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  val training = sqlContext
    .jsonFile(trainingInputFile) // Load training data from JSON file
    .select("category", "features") // Select interesting column for training
    .map(row => LabeledPoint(row.getDouble(0), Vectors.dense(row.getSeq[Double](1).toArray))) // Transform row into somethinng that can be swallowed by ML training algorithm

  val test = sqlContext
    .jsonFile(testInputFile) // Load test data from JSON file
    .select("name", "category", "features") // Select interesting columns

  val model = LogisticRegressionWithSGD.train(training, 1000, 1000) // Feed a logistic regression algorithm with training data (iteration : 1000, step size : 1000)

  import sqlContext.implicits._ // import SQL implicits to have toDF method

  val labeledCities:RDD[String] = test
    .map(row => {
    val prediction = model.predict(Vectors.dense(row.getSeq[Double](2).toArray))
    (row.getString(0), row.getDouble(1), prediction)
  }) // Label cities in the test set
    .toDF("name", "category", "prediction") // Select interesting columns
    .toJSON // Transform to JSON String RDD

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  sparkContext.stop() // Stop connection to Spark

}


