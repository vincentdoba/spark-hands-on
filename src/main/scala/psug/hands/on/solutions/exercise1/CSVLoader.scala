package psug.hands.on.solutions.exercise1

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.exercise1.Winner._

trait CSVLoader {

  def getCSV(inputFile: String) = {

    val sqlContext = new SQLContext(sparkContext)

    sqlContext.load("com.databricks.spark.csv", Map("path" -> inputFile, "header" -> "true"))
  }

}
