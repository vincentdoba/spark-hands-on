package psug.hands.on.examples.notSerializableException

import org.apache.spark.{SparkContext, SparkConf}

object RDDCreator {

  def createRDD(list:List[Int]) = {
    val conf = new SparkConf().setMaster("local").setAppName("application")
    val sparkContext = new SparkContext(conf)
    sparkContext.makeRDD(list)
  }

}
