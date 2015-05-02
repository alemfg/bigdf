/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         Some utility functions that have no good home
 */
package com.ayasdi.bigdf

import java.io.File

import org.apache.log4j.{Logger, Level}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

private[bigdf] object CountHelper {
  def countNaN(row: Array[Any]) = {
    var ret = 0
    for (col <- row) {
      val a = col match {
        case x: Double => x.isNaN
        case x: String => x.isEmpty
        case x: Short => x == RichColumnCategory.CATEGORY_NA //short is used for category
        case x: Float => x.isNaN
      }
      if (a == true) ret += 1
    }
    ret
  }
}

/*
 * needed this to work around task serialization failure in spark
 */
private[bigdf] case class PivotHelper(grped: RDD[(Any, Iterable[Array[Any]])],
                                      pivotIndex: Int,
                                      pivotValue: String) {
  def get = {
    grped.map {
      case (k, v) =>
        val vv = v.filter { row =>
          row(pivotIndex) match {
            case cellD: Double => cellD.toString == pivotValue
            case cellS: String => cellS == pivotValue
          }
        }
        (k, vv)
    }

  }
}

object FileUtils {
  def removeAll(path: String) = {
    def getRecursively(f: File): Seq[File] =
      f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles ++ List(f)


    val file = new File(path)
    if(file.exists()) {
      getRecursively(file).foreach { f =>
        if (!f.delete())
          throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
      }
    }
  }

  def dirToFiles(path: String, recursive: Boolean = true)(implicit sc: SparkContext) = {
    import org.apache.hadoop.fs._
    import scala.collection.mutable.MutableList
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val files = fs.listFiles(new Path(path), recursive)
    val fileList = MutableList[String]()
    while(files.hasNext) {
      val file = files.next
      if(file.isFile) fileList += file.getPath.toUri.getPath
    }

    fileList.toList
  }

  def isDir(path: String)(implicit sc: SparkContext) = {
    import org.apache.hadoop.fs._
    val fs = FileSystem.get(sc.hadoopConfiguration)

    fs.isDirectory(new Path(path))
  }

}

object SparkUtil {
  def silenceSpark {
    setLogLevels(Level.WARN, Seq("spark", "org", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map {
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }
}
