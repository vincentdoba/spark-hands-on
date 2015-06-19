package psug.hands.on.solutions.exercise08.sparkML

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
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
 * command : sbt "run-main psug.hands.on.solutions.exercise08.sparkML.MachineLearning"
 */
object MachineLearning extends App with SparkContextInitiator with DataSaver {

  val trainingInputFile = "data/training_cities.json"
  val testInputFile = "data/test_cities.json"
  val outputFile = "data/labeled_cities.json"

  init()

  val sparkContext = initContext("machineLearningSparkML") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  import org.apache.spark.sql.functions._ // import function to get udf function

  val toVector = udf[Vector, Seq[Double]](seq => Vectors.dense(seq.toArray)) // function to transform Sequence of Double to Vector

  val training = sqlContext.read.json(trainingInputFile).select("category", "features") // Load training data from JSON File
  val test = sqlContext.read.json(testInputFile).select("name", "category", "features") // Load test data from JSON File

  val logisticRegression = new LogisticRegression()
    .setMaxIter(100) // The number of iteration
    .setRegParam(0.001) // The step size
    .setFeaturesCol("features") // The column that contains features
    .setLabelCol("category") // The column that contains label

  val pipeline = new Pipeline()
    .setStages(Array(logisticRegression)) // Create Machine Learning Pipeline

  val model = pipeline.fit(training.select(training("category"), toVector(training("features")).as("features"))) // Feed the Logistic Regression algorithm with training data to create a ML model

  val predictions = model.transform(test.select(test("name"), test("category"), toVector(test("features")).as("features"))) // Label test data with model

  val labeledCities: RDD[String] = predictions.select("name", "category", "prediction").toJSON // Transform results of prediction to JSON String RDD

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  sparkContext.stop() // Stop connection to Spark

}


