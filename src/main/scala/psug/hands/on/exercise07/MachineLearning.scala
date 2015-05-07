package psug.hands.on.exercise07

import org.apache.spark.sql.DataFrame

/**
 * Apply a Linear Regression model trained using 500 cities picked randomly among the list of cities having more than
 * 2000 inhabitants in France on the rest of the cities having more than 2000 inhabitants in order to determine which
 * cities have more than 5000 inhabitants.
 *
 * Display the precision of the algorithm (number of good guess over total number of cities) and ten cities that
 * were mislabeled
 *
 * file : normalized_features.json
 *
 * command : sbt "run-main psug.hands.on.exercise07.MachineLearning"
 */
object MachineLearning extends App {

  val inputFile = "data/normalized_features.json"

  val trainingData:DataFrame = ??? // TODO create training data (randomly choose 500 cities in inputFile)
  val testData:DataFrame = ??? // TODO create test data (cities in inputFile that are not in training data)
  // TODO initialize Linear Regression algorithm
  // TODO train linear regression model with training data
  // TODO classify test data using linear regression model
  val accuracy:Long = ??? // TODO retrieve accuracy percentage a two digit long
  val misLabeledCitiesExamples:Iterable[String] = ??? // TODO retrieve ten mislabeled cities

  println("Accuracy is " + accuracy + "%")
  println("Examples of cities mislabeled : " + misLabeledCitiesExamples.mkString(", "))

}
