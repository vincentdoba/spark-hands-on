package psug.hands.on.examples.notSerializableException

import org.apache.spark.rdd.RDD

/*
 * Corrected Incrementor in order to not throw NotSerializableException
 *
 * command : sbt "run-main psug.hands.on.examples.notSerializableException.Working"
 */
object Working extends App {

  val rdd = RDDCreator.createRDD(List(1,2,3))

  new GoodIncrementor(3).transform(rdd).foreach(println)
}

class GoodIncrementor(i:Int) {

  def transform(rdd:RDD[Int]) = {
    val i_ = i
    rdd.map(a => a + i_)
  }
}
