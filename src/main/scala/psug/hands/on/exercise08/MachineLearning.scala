package psug.hands.on.exercise08

import org.apache.spark.rdd.RDD
import psug.hands.on.exercise05.{City, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * - Train a Linear Regression model using training_cities.json
 * - Predict for each city in test_cities.json if it has more than 5000 inhabitants
 * - Save result in a labeled_cities.json file
 *
 * input file 1 : data/training_cities.json
 * input file 2 : data/test_cities.json
 * output file : data/labeled_cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise08.MachineLearning"
 */
object MachineLearning extends App with DataSaver {

  val trainingInputFile = "data/training_cities.json"
  val testInputFile = "data/test_cities.json"
  val outputFile = "data/labeled_cities.json"

  init()

  val labeledCities: RDD[String] = ???

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)
}
