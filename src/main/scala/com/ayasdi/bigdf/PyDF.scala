/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package com.ayasdi.bigdf

import collection.JavaConversions._
import org.apache.spark.BigDFPyRDD
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}
import implicits._
import java.util.{ArrayList => JArrayList}
import scala.collection.JavaConverters._

case class PyDF(df: DF) {
  def columnNames = df.columnNames

  def column(name: String) = PyColumn(df.column(name))

  def list(numRows: Int, numCols: Int) = df.list(numRows, numCols)

  def where(predicate: PyPredicate): PyDF = {
    PyDF(df(predicate.p))
  }

  def update(name: String, pycol: PyColumn[_]) = {
    df.update(name, pycol.col)
  }
  
  def rowCount = df.rowCount
  
  def colCount = df.colCount
  
  def aggregate(byColumnJ: JArrayList[String], aggrColumnJ: JArrayList[String], aggregator: String) : PyDF = {
    val byColumn = byColumnJ.asScala.toList
    val aggrColumn = aggrColumnJ.asScala.toList
    val dfAgg = aggregator match {
      case "Mean" => df.aggregate(byColumn, aggrColumn, AggMean)
      case "Count" => df.aggregate(byColumn, aggrColumn, AggCount)
      case "StrJoin" => df.aggregate(byColumn, aggrColumn, new AggMakeString(","))
      case _ => null
    }
    PyDF(dfAgg)
  }
  
  def groupBy(colName: String) = df.groupBy(colName)
  
  def pivot(keyCol: String, pivotByCol: String,
            pivotedCols: JArrayList[String]): PyDF = {
    PyDF(df.pivot(keyCol, pivotByCol, pivotedCols.asScala.toList))
  }
  
  def writeToCSV(file: String, separator: String = ","): Unit = df.writeToCSV(file, separator)
}

object PyDF {
  def fromCSV(sc: SparkContext, name: String, separator: String, fasterGuess: Boolean, nParts: Int = 0): PyDF = {
    val sep: Char = separator.charAt(0)
    PyDF(DF(sc, name, sep, nParts))
  }
}

case class PyColumn[+T: ru.TypeTag](col: Column[T]) {
  def list(numRows: Int) = col.list(numRows)
  override def toString = {
    val name = s"${col.rdd.name}".split('/').last.split('.').head
    s"$name\t${col.colType}"
  }
  
  def tpe = s"${col.colType}"

  def stats = {
    col.colType match {
      case ColType.Double | ColType.Float | ColType.Short =>
        val rcol: RichColumnDouble = col
        val stats = rcol.stats

        mapAsJavaMap(Map("min" -> stats.min,
            "max" -> stats.max,
            "mean" -> stats.mean,
            "std" -> stats.stdev,
            "var" -> stats.variance,
            "count" -> stats.count
            ))
      case _ => null
    }
  }

  def javaToPython : JavaRDD[Array[Byte]] = {
    col.colType match {
      case ColType.Double => BigDFPyRDD.pythonRDD(col.rdd)      
      case _ => null
    }
  }

  def pythonToJava(c : JavaRDD[Array[Byte]]) = {
      val jrdd : JavaRDD[Double] = BigDFPyRDD.javaRDD(c)
      val newCol = Column(col.sc, jrdd.rdd, 0)
      PyColumn(newCol)
  }

  def add(v: Double) = {
    val rcol : RichColumnDouble = col
    PyColumn(rcol + v)
  }

  def sub(v: Double) = {
    val rcol : RichColumnDouble = col
    PyColumn(rcol - v)
  }

  def mul(v: Double) = {
    val rcol : RichColumnDouble = col
    PyColumn(rcol * v)
  }

  def div(v: Double) = {
    val rcol : RichColumnDouble = col
    PyColumn(rcol / v)
  }

}

case class PyPredicate(p: Predicate) {
  def And(that: PyPredicate) = {
    PyPredicate(this.p && that.p)
  }

  def Or(that: PyPredicate) = {
    PyPredicate(this.p || that.p)
  }

  def Not() = {
    PyPredicate(!this.p)
  }
}

object PyPredicate {
  def where[T](column: PyColumn[T], operator: String, value: Double): PyPredicate = {
    val filter = operator match {
      case "==" => column.col === value
      case "!=" => column.col != value
      case "<" => column.col < value
      case "<=" => column.col <= value
      case ">" => column.col > value
      case ">=" => column.col >= value
    }
    PyPredicate(filter)
  }

  def where[T](column: PyColumn[T], operator: String, value: String): PyPredicate = {
    val filter = operator match {
      case "==" => column.col === value
      case "!=" => column.col != value
      case "<" => column.col < value
      case "<=" => column.col <= value
      case ">" => column.col > value
      case ">=" => column.col >= value
    }
    PyPredicate(filter)
  }
}


