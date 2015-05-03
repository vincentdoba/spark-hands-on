package psug.hands.on.solutions.exercise02

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Determine the total population in France
 *
 * Déterminer la population totale de la France
 *
 * file : data/demographie_par_commune.json
 * fichier : data/demographie_par_commune.json
 *
 */
object TotalPopulation extends App with SparkContextInitiator {

  val inputFile = args(0)

  val sparkContext = initContext("totalPopulation")
  val sqlContext = new SQLContext(sparkContext)

  val input = sqlContext.jsonFile(inputFile)

  val population = input.filter("Population > 0").select("Population").map( row => row.getLong(0)).reduce((a,b) => a + b)

  println("La France compte " + population + " habitants")

  sparkContext.stop()

}

