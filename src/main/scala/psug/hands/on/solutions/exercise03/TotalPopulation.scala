package psug.hands.on.solutions.exercise03

import org.apache.spark.sql.{DataFrame, SQLContext}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Determine the total population in France
 *
 * input file : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise03.TotalPopulation"
 *
 */
object TotalPopulation extends App with SparkContextInitiator {

  val inputFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("totalPopulation") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  val input:DataFrame = sqlContext.read.json(inputFile) // Load a Json File into a Data Frame

  import org.apache.spark.sql.functions._ // Import functions such as "sum"
  val population = input
      .filter("Population > 0") // Filter by population > 0
      .agg(sum("Population")) // Aggregate by doing the sum of column Population
      .first() // Take the first row of the data frame
      .getLong(0) // Retrieve the first column of the row

  println("La France compte " + population + " habitants")

  sparkContext.stop() // stop connection to Spark

}

