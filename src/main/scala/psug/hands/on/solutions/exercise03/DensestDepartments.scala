package psug.hands.on.solutions.exercise03

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.SparkContextInitiator

object DensestDepartments extends App with SparkContextInitiator {

  val dataFile = args(0)
  val departmentsFile = args(1)

  val sparkContext = initContext("densestDepartment")
  val sqlContext = new SQLContext(sparkContext)

  val data = sqlContext.jsonFile(dataFile)

  val densestDepartmentsCodes = data
    .filter("Population > 0")
    .filter("Superficie > 0")
    .select("Departement", "Population", "Superficie")
    .map(row => (row.getString(0),(row.getLong(1), row.getLong(2))))
    .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
    .map(tuple => (tuple._1, tuple._2._1 / tuple._2._2))
  
  val departments = sparkContext.textFile(departmentsFile)

  val departmentsNamesByCode = departments
    .map(line => line.split(","))
    .map(a => (a(1), a(0)))

  val densestDepartmentsNames = densestDepartmentsCodes
    .join(departmentsNamesByCode)
    .sortBy(_._2._1, false)
    .map(_._2._2)
    .take(10)

  println("Les départements les plus densément peuplés sont " + densestDepartmentsNames.mkString(", "))

  sparkContext.stop()


}
