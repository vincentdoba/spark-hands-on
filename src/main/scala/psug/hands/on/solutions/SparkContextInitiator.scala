package psug.hands.on.solutions

import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextInitiator {

  def initContext(applicationName: String) = {
    val conf = new SparkConf().setMaster("local").setAppName(applicationName)
    new SparkContext(conf)
  }

}
