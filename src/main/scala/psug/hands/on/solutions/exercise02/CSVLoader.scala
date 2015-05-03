package psug.hands.on.solutions.exercise02

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.exercise02.ElectionWinner._

trait CSVLoader {

  def getCSV(inputFile: String) = {

    val sqlContext = new SQLContext(sparkContext)

    sqlContext.load("com.databricks.spark.csv", Map("path" -> inputFile, "header" -> "true"))
  }

}
