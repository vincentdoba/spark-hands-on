package psug.hands.on.solutions.exercise1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Determine who is the winner of the 2012 French presidential election
 *
 * Déterminer qui est le gagnant de l'élection présidentielle de 2012
 *
 * File : data/resultat_presidentielles_par_commune_2012.csv
 *
 */
object Winner extends App {

  val inputFile = args(0)

  val conf = new SparkConf().setAppName("Winner")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> inputFile, "header" -> "true"))
  println(df.columns)



}
