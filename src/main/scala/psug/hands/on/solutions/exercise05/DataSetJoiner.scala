package psug.hands.on.solutions.exercise05

import java.text.NumberFormat
import java.util.Locale

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.SparkContextInitiator

object DataSetJoiner extends App with SparkContextInitiator {

  val format = NumberFormat.getInstance(Locale.FRANCE)
  
  val votesFile = "data/resultat_presidentielles_par_commune_2012.csv"
  val populationDataFile = "data/demographie_par_commune.json"
  
  
  val sparkContext = initContext("dataSetJoiner")

  val sqlContext = new SQLContext(sparkContext)

  val votesDataFrame = sqlContext.load("com.databricks.spark.csv", Map("path" -> votesFile, "header" -> "true"))
  val populationDataFrame = sqlContext.jsonFile(populationDataFile)

  populationDataFrame.registerTempTable("population")
  val filteredPopulationDataFrame = sqlContext.sql("SELECT * FROM population WHERE CodeInsee IS NOT NULL")

  val hollandeVotes = votesDataFrame
    .select("Code du département", "Code de la commune", "% Voix/Exp 1")
    .map(row => (transformToInseeCode(row.getString(0), row.getString(1)), format.parse(row.getString(2)).doubleValue()))



  def transformToInseeCode(departmentCode:String, townCode:String):String = {
    val departmentPart = 2 - departmentCode.length match {
      case 1 => "0" + departmentCode
      case _ => departmentCode
    }

    val townPart = 5 - departmentPart.length - townCode.length match {
      case 2 => "00" + townCode
      case 1 => "0" + townCode
      case _ => townCode
    }

    departmentPart + townPart

  }

  sparkContext.stop()

}
