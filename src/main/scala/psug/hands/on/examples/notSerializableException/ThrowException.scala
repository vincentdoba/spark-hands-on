package psug.hands.on.examples.notSerializableException

import org.apache.spark.rdd.RDD

/**
 * Throw exception due to an incrementor that is serialized
 *
 * command : sbt "run-main psug.hands.on.examples.notSerializableException.ThrowException"
 */
object ThrowException extends App {

  val rdd = RDDCreator.createRDD(List(1,2,3))

  new BadIncrementor(3).transform(rdd).foreach(println)
}

class BadIncrementor(i:Int) {

  def transform(rdd:RDD[Int]) = {
    rdd.map(a => a + i)
  }
}
