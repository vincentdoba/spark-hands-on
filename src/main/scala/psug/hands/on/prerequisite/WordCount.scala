package psug.hands.on.prerequisite

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val inputFile = args(0)

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("wordCount")
  val sparkContext = new SparkContext(conf)

  val input =  sparkContext.textFile(inputFile)
  val words = input.flatMap(line => line.split(" "))
  val counts = words.map(word => (word, 1))
    .reduceByKey{case (x, y) => x + y}
  val topTen: Array[(String, Int)] = counts.sortBy(_._2, false)
    .take(10)

  topTen.map(println)
  
  sparkContext.stop()
}