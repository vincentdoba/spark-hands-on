package psug.hands.on.exercise06

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
 * Normalize features retrieved in previous exercice 05 so a Machine Learning algorithm can swallow them
 *
 * file : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise06.Normalization"
 *
 */
object Normalization extends App with DataSaver {

}

trait DataSaver {

  val temporaryFile = "/tmp/spark_temp_files"

  def init(destinationFile:String) {
    FileUtil.fullyDelete(new File(temporaryFile))
    FileUtil.fullyDelete(new File(destinationFile))

  }

  def merge(srcPath: String, dstPath: String) {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}
