package psug.hands.on.prerequisite

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val inputFile = args(0)

  val conf = new SparkConf().setMaster("local").setAppName("wordCount")
  val sparkContext = new SparkContext(conf)

  // Load our input data.
  val input =  sparkContext.textFile(inputFile)
  // Split it up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Take the top 10 words
  val topTen: Array[(String, Int)] = counts.sortBy(_._2, false).take(10)
  // Print result
  topTen.map(println)
  
  sparkContext.stop()
}