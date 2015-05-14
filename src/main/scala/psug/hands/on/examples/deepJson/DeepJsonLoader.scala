package psug.hands.on.examples.deepJson

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Try to load a deep JSON
 *
 * command : sbt "run-main psug.hands.on.examples.deepJson.DeepJsonLoader"
 */
object DeepJsonLoader extends App {

  val inputFile = "data/deep_json_example.json"

  val sparkConf = new SparkConf().setMaster("local").setAppName("deepJsonLoader")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val loadedInput = sqlContext.jsonFile(inputFile)

  println("==== DataFrame Structure ====")
  println()
  loadedInput.printSchema()

  println("==== Data ====")
  println()
  loadedInput.show()

  println()
  println("==== Retrieve a deep element ====")
  println()
  println("loadedInput.select(\"firstLayer1.secondLayer1\").first() gives : " + loadedInput.select("firstLayer1.secondLayer1").first().getString(0))

  sparkContext.stop()


}
