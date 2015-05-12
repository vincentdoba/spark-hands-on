package psug.hands.on.solutions

import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextInitiator {

  def initContext(applicationName: String) = {
    val conf = new SparkConf()
      .setMaster("local[2]") // Create conf with 2 thread on local machine
      .setAppName(applicationName) // Set an application name
    new SparkContext(conf) // Create Spark Context given conf
  }

}
