package psug.hands.on.solutions.exercise08

import org.apache.spark.sql.Row


trait MLHelpers {

  def displayResults(resultStats: MachineLearningStats, mislabeledExamples: Array[String]): Unit = {
    val accuracy: Long = resultStats.correctlyLabeled * 100 / resultStats.total
    val precision: Long = resultStats.positiveCorrectlyLabeled * 100 / resultStats.positive
    val recall: Long = resultStats.positiveCorrectlyLabeled * 100 / resultStats.positiveLabeled

    println("Total well labeled cities percentage is " + accuracy + "%")
    println("Precision percentage for >5000 cities category is " + precision + "%")
    println("Recall percentage for >5000 cities category is " + recall + "%")
    println("F-Score for >5000 cities is " + (2 * recall * precision / (precision + recall)) + " %")
    println("Examples of cities mislabeled : " + mislabeledExamples.mkString(", "))
  }

  def extractStats(row: Row):MachineLearningStats = extractStats(row.getString(0), row.getDouble(1), row.getDouble(2))

  def stringifyResultRow(row:Row):String = stringifyResultRow(row.getString(0), row.getDouble(1), row.getDouble(2))

  def extractStats(result: (String, Double, Double)) = {
    val total = 1
    val correctlyLabeled = if (result._2 == result._3) 1 else 0
    val positive = result._2.toInt
    val positiveCorrectlyLabeled = (result._2*result._3).toInt
    val positiveLabeled = result._3.toInt
    MachineLearningStats(total, correctlyLabeled, positive, positiveCorrectlyLabeled, positiveLabeled)
  }

  def stringifyResultRow(result: (String, Double, Double)) = (result._2, result._3) match {
    case (1, 0) => result._1 + " (actual >5000, labeled <5000)"
    case (0, 1) => result._1 + " (actual <5000, labeled >5000)"
    case (a, b) if a==b => println(result); "error, " + result._1 + " is well labeled"
    case _ => "error, unexpected labeling"
  }

}

case class MachineLearningStats(total: Int, correctlyLabeled: Int, positive: Int, positiveCorrectlyLabeled: Int, positiveLabeled:Int) {

  def aggregate(that: MachineLearningStats) = MachineLearningStats(
    this.total + that.total,
    this.correctlyLabeled + that.correctlyLabeled,
    this.positive + that.positive,
    this.positiveCorrectlyLabeled + that.positiveCorrectlyLabeled,
    this.positiveLabeled + that.positiveLabeled
  )

}
