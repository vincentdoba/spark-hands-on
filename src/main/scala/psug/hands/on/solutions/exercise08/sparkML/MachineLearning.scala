package psug.hands.on.solutions.exercise08.sparkML

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import psug.hands.on.exercise05.DataSaver
import psug.hands.on.solutions.SparkContextInitiator
import psug.hands.on.solutions.exercise08.MLHelpers

/**
 * Apply a Linear Regression model trained using 500 cities picked randomly among the list of cities having more than
 * 2000 inhabitants in France on the rest of the cities having more than 2000 inhabitants in order to determine which
 * cities have more than 5000 inhabitants.
 *
 * Display the precision of the algorithm (number of good guess over total number of cities) and ten cities that
 * were mislabeled
 *
 * input file : normalized_cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise08.sparkML.MachineLearning"
 */
object MachineLearning extends App with SparkContextInitiator with MLHelpers with DataSaver {

  val trainingInputFile = "data/training_cities.json"
  val testInputFile = "data/test_cities.json"
  val outputFile = "data/labeled_cities.json"

  init()

  val sparkContext = initContext("machineLearningSparkML")
  val sqlContext = new SQLContext(sparkContext)

  import org.apache.spark.sql.functions._

  val toVector = udf[Vector, Seq[Double]](seq => Vectors.dense(seq.toArray))

  val training = sqlContext.jsonFile(trainingInputFile).select("category", "features")
  val test = sqlContext.jsonFile(testInputFile).select("name", "category", "features")

  val logisticRegression = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
    .setFeaturesCol("features")
    .setLabelCol("category")

  val pipeline = new Pipeline()
    .setStages(Array(logisticRegression))

  val model = pipeline.fit(training.select(training("category"), toVector(training("features")).as("features")))

  val predictions = model.transform(test.select(test("name"), test("category"), toVector(test("features")).as("features")))

  val labeledCities: RDD[String] = predictions.select("name", "category", "prediction").toJSON

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  sparkContext.stop()

}


