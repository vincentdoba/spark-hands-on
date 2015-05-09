package psug.hands.on.exercise07

import org.apache.spark.rdd.RDD
import psug.hands.on.exercise05.DataSaver

object DataSetSplitter extends App with DataSaver {

  val inputFile = "data/normalized_cities.json"
  val trainingCitiesFile = "data/training_cities.json"
  val testCitiesFile = "data/test_cities.json"

  val trainingCities:RDD[String] = ???
  val testCities:RDD[String] = ???

  trainingCities.saveAsTextFile(temporaryFile + 1)
  merge(temporaryFile + 1, trainingCitiesFile)

  testCities.saveAsTextFile(temporaryFile + 2)
  merge(temporaryFile + 2, testCitiesFile)

  println("There are " + testCities.count() + " test cities and " + trainingCities.count() + " training cities")

}
