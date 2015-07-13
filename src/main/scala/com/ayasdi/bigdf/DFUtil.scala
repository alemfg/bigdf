/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         Some utility functions that have no good home
 */
package com.ayasdi.bigdf

import java.io.File

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

private[bigdf] object CountHelper {
  def countNaN(row: Array[Any]) = {
    var ret = 0
    for (col <- row) {
      val a = col match {
        case x: Double => x.isNaN
        case x: String => x.isEmpty
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
    if (file.exists()) {
      getRecursively(file).foreach { f =>
        if (!f.delete())
          throw new RuntimeException("Failed to delete " + f.getAbsolutePath)
      }
    }
  }

  def dirToFiles(path: String, recursive: Boolean = true, pattern: String)(implicit sc: SparkContext) = {
    import scala.collection.mutable.MutableList

    import org.apache.hadoop.fs._

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val files = fs.listFiles(new Path(path), recursive)
    val fileList = MutableList[String]()
    val regex = pattern.r
    while (files.hasNext) {
      val file = files.next
      if (file.isFile) {
        val path = file.getPath.toUri.getPath
        val matched = regex.findFirstIn(path)
        if(matched.nonEmpty && matched.get == path)
          fileList += file.getPath.toUri.getPath
      }
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
  def silenceSpark(): Unit = {
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

  def sqlToColType(sqlType: DataType): ColType.EnumVal = sqlType match {
    case DoubleType => ColType.Double
    case FloatType => ColType.Float
    case ShortType => ColType.Short
    case LongType => ColType.Long
    case StringType | BinaryType => ColType.String
    case ArrayType(DoubleType, _) => ColType.ArrayOfDouble
    case ArrayType(StringType, _) => ColType.ArrayOfString
    case MapType(StringType, FloatType, _) => ColType.MapOfStringToFloat
    case MapType(StringType, LongType, _) => ColType.MapOfStringToLong
    case _ => throw new Exception(s"Unsupported type: $sqlType")
  }

  def colTypeToSql(colType: ColType.EnumVal): DataType = colType match {
    case ColType.Double => DoubleType
    case ColType.Float => FloatType
    case ColType.Short => ShortType
    case ColType.Long => LongType
    case ColType.String => StringType
    case ColType.ArrayOfDouble => ArrayType(DoubleType)
    case ColType.ArrayOfString => ArrayType(StringType)
    case ColType.MapOfStringToFloat => MapType(StringType, FloatType)
    case ColType.MapOfStringToLong => MapType(StringType, LongType)
    case _ => throw new Exception(s"Unsupported type: $colType")
  }

  def typeTagToSql(tpe: ru.Type) = if (tpe =:= ru.typeOf[Double]) DoubleType
    else if (tpe =:= ru.typeOf[Float]) FloatType
    else if (tpe =:= ru.typeOf[String]) StringType
    else if (tpe =:= ru.typeOf[Long]) LongType
    else if (tpe =:= ru.typeOf[Short]) ShortType
    else if (tpe =:= ru.typeOf[ArrayBuffer[Double]]) ArrayType(DoubleType)
    else if (tpe =:= ru.typeOf[ArrayBuffer[Float]]) ArrayType(FloatType)
    else if (tpe =:= ru.typeOf[ArrayBuffer[String]]) ArrayType(StringType)
    else if (tpe =:= ru.typeOf[Map[String, Float]]) MapType(StringType, FloatType)
    else if (tpe =:= ru.typeOf[mutable.Map[String, Float]]) MapType(StringType, FloatType)
    else if (tpe =:= ru.typeOf[Map[String, Long]]) MapType(StringType, FloatType)
    else if (tpe =:= ru.typeOf[mutable.Map[String, Long]]) MapType(StringType, LongType)
    else throw new IllegalArgumentException(s"Type not supported: $tpe")

  def typeTagToClassTag[V: ru.TypeTag] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    ClassTag[V](mirror.runtimeClass(ru.typeOf[V]))
  }

}

object KeyMaker {
  def makeKey(a: Array[Any]) = a.toList
}
