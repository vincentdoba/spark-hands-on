package psug.hands.on.exercise05

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

trait DataSaver {

  val temporaryFile = "tmp/spark_temp_files"

  def init() {
    FileUtil.fullyDelete(new File(temporaryFile))
  }

  def merge(srcPath: String, dstPath: String) {
    FileUtil.fullyDelete(new File(dstPath))
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    deleteCrcFile(dstPath)
  }

  private def deleteCrcFile(path: String) {
    val pathElements = path.split("/").reverse
    val crcFileName = "." + pathElements.head + ".crc"
    val directoryPath = pathElements.tail.reverse match {
      case Array() => ""
      case x => x.mkString("/") + "/"
    }
    val crcFilePath = directoryPath + crcFileName
    FileUtil.fullyDelete(new File(crcFilePath))

  }

}
