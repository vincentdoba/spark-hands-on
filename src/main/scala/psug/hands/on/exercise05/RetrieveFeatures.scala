package psug.hands.on.exercise05

import org.apache.spark.rdd.RDD

/**
 * Save the following information in a JSON file :
 *
 * - City name
 * - If the city has more than 5000 inhabitants
 * - City density
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 *
 *
 * file : data/demographie_par_commune.json
 * output file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise05.RetrieveFeatures"
 *
 */
object RetrieveFeatures extends App  with DataSaver {

  val inputFile = "data/demographie_par_commune.json"
  val outputFile = "data/cities.json"

  init()

  val cities:RDD[String] = ??? // TODO generate cities in France and their characteristics (name, has more than 5000 inhabitants...) JSON Strings

  cities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/cities.json : ")
  cities.take(10).foreach(println)


}
