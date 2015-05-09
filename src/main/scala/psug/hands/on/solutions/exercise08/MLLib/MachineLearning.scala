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

  val sparkContext = initContext("machineLearningMLLib")
  val sqlContext = new SQLContext(sparkContext)

  val training = sqlContext
    .jsonFile(trainingInputFile)
    .select("category", "features")
    .map(row => LabeledPoint(row.getDouble(0), Vectors.dense(row.getSeq[Double](1).toArray)))
    .cache()

  val test = sqlContext
    .jsonFile(testInputFile)
    .select("name", "category", "features")
    .cache()

  val model = LogisticRegressionWithSGD.train(training, 500, 0.01)

  import sqlContext.implicits._

  val labeledCities:RDD[String] = test
    .map(row => {
    val prediction = model.predict(Vectors.dense(row.getSeq[Double](2).toArray))
    (row.getString(0), row.getDouble(1), prediction)
  })
    .toDF("name", "category", "prediction")
    .toJSON

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  sparkContext.stop()

}


