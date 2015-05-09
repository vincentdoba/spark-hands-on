package psug.hands.on.solutions.exercise01

import psug.hands.on.solutions.SparkContextInitiator

/**
 * Compute the sum of squares of numbers that are under 100 and that are not prime.
 *
 * For instance, the same sum but for numbers that are under 10 is 1² + 4² + 6² + 8² + 9² + 10² = 308
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise01.SumOfSquaresOfNonPrimeNumbers"
 *
 */
object SumOfSquaresOfNonPrimeNumbers extends App with SparkContextInitiator {

  val startingNumbersList = 1 to 75
  val endingNumbersList = 25 to 100
  val primeNumbersList = List(2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)

  val sparkContext = initContext("sumOfSquareOfNonPrimeNumbers")
  
  val startingNumbers = sparkContext.makeRDD(startingNumbersList)
  val endingNumbers = sparkContext.makeRDD(endingNumbersList)
  val primeNumbers = sparkContext.makeRDD(primeNumbersList)

  val sumOfSquareOfNonPrimeNumbers = startingNumbers
    .union(endingNumbers)
    .distinct()
    .subtract(primeNumbers)
    .map(x => x*x)
    .reduce((a, b) => a + b)

  println(s"The sum of square of numbers that are not prime and are under 100 is $sumOfSquareOfNonPrimeNumbers")

  sparkContext.stop()

}
