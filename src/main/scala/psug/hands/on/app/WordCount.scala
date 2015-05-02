package psug.hands.on.app

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val inputFile = args(0)
  val outputFile = args(1)

  val conf = new SparkConf().setAppName("wordCount")
  val sc = new SparkContext(conf)

  // Load our input data.
  val input =  sc.textFile(inputFile)
  // Split it up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
  counts.saveAsTextFile(outputFile)
}
