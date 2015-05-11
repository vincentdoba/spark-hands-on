package psug.hands.on.exercise01

/**
 * Compute the sum of squares of numbers that are under 100 and that are not prime.
 *
 * For instance, the same sum but for numbers that are under 10 is 1^2 + 4^2 + 6^2 + 8^2 + 9^2 + 10^2 = 298
 *
 * command : sbt "run-main psug.hands.on.exercise01.SumOfSquaresOfNonPrimeNumbers"
 *
 */
object SumOfSquareOfNonPrimeNumbers extends App {

  val startingNumbersList = 1 to 75
  val endingNumbersList = 25 to 100
  val primeNumbersList = List(2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)

  val sumOfSquareOfNonPrimeNumbers = ??? // TODO compute the sum of squares of numbers under 100 that are not prime

  println(s"The sum of square of numbers that are not prime and are under 100 is $sumOfSquareOfNonPrimeNumbers")

}
