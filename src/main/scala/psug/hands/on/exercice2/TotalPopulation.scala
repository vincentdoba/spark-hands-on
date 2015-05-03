package psug.hands.on.exercice2

import org.apache.spark.{SparkConf, SparkContext}

object TotalPopulation extends App {

  val inputFile = args(0)

  val conf = new SparkConf().setAppName("totalPopulation")
  val sc = new SparkContext(conf)


}
