package psug.hands.on.solutions.exercise03

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.exercise02.TotalPopulation._

object DensestDepartments extends App {

  val inputFile = args(0)

  val sparkContext = initContext("densestDepartment")
  val sqlContext = new SQLContext(sparkContext)

  val input = sqlContext.jsonFile(inputFile)

  val densestDepartment = input
    .filter("Population > 0")
    .filter("Superficie > 0")
    .select("Departement", "Population", "Superficie")
    .map(row => (row.getString(0),(row.getLong(1), row.getLong(2))))
    .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
    .map(tuple => (tuple._1, tuple._2._1 / tuple._2._2))
    .sortBy(_._2, false)
    .map(_._1)
    .take(10)

  println("Les départements les plus densément peuplés sont " + densestDepartment.mkString(","))

  sparkContext.stop()


}
