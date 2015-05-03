package psug.hands.on.solutions

import org.apache.spark.{SparkContext, SparkConf}

trait SparkContextInitiator {

  def initContext(applicationName: String) = {
    val conf = new SparkConf().setMaster("local").setAppName(applicationName)
    new SparkContext(conf)
  }

}
