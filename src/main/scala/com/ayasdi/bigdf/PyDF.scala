/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package com.ayasdi.bigdf

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect._
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{BigDFPyRDD, SparkContext}
import com.ayasdi.bigdf.Implicits._

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

  def colCount = df.columnCount

  def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType) = {
    DF.join(sc, left, right, on, how)
  }

  def aggregate(byColumnJ: JArrayList[String], aggrColumnJ: JArrayList[String], aggregator: String): PyDF = {
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
  def fromCSV(sc: SparkContext, name: String, separator: String, fasterGuess: Boolean, nParts: Int): PyDF = {
    val sep: Char = separator.charAt(0)
    PyDF(DF(sc, name, sep, nParts, Options()))
  }
  def fromColumns(sc: SparkContext, pycols: JArrayList[PyColumn[Any]], name: String): PyDF = {
    val cols = pycols.map(_.col)
    PyDF(DF.fromColumns(sc, cols, name, Options()))
  }
  
}

case class PyColumn[+T: ru.TypeTag](col: Column[T]) {
  def list(numRows: Int) = col.list(numRows)

  override def toString = {
    val name = s"${col.rdd.name}".split('/').last.split('.').head
    s"$name\t${col.colType}"
  }

  def setName(name: String): Unit = { col.name = name }
  
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

  def javaToPython: JavaRDD[Array[Byte]] = col.colType match {
    case ColType.Double => BigDFPyRDD.pythonRDD(col.doubleRdd)
    case ColType.String => BigDFPyRDD.pythonRDD(col.stringRdd)
    case ColType.Float => BigDFPyRDD.pythonRDD(col.floatRdd)
    case ColType.ArrayOfDouble => BigDFPyRDD.pythonRDD(col.arrayOfDoubleRdd)
    case ColType.ArrayOfString => BigDFPyRDD.pythonRDD(col.arrayOfStringRdd)
    case ColType.Short => BigDFPyRDD.pythonRDD(col.shortRdd)
    case ColType.MapOfStringToFloat => BigDFPyRDD.pythonRDD(col.mapOfStringToFloatRdd)
    case ColType.Undefined => throw new IllegalArgumentException("Undefined column type")
  }

  def pythonToJava[T: ClassTag](c: JavaRDD[Array[Byte]]): PyColumn[Any] = {
    val jrdd: JavaRDD[T] = BigDFPyRDD.javaRDD(c)
    val tpe = classTag[T]
    if (tpe == classTag[Double]) PyColumn[Double](Column(col.sc, jrdd.rdd.asInstanceOf[RDD[Double]], -1))
    else if (tpe == classTag[String]) PyColumn(Column(col.sc, jrdd.rdd.asInstanceOf[RDD[String]], -1))
    else null
  }

  def add(v: Double) = {
    val rcol: RichColumnDouble = col
    PyColumn(rcol + v)
  }

  def sub(v: Double) = {
    val rcol: RichColumnDouble = col
    PyColumn(rcol - v)
  }

  def mul(v: Double) = {
    val rcol: RichColumnDouble = col
    PyColumn(rcol * v)
  }

  def div(v: Double) = {
    val rcol: RichColumnDouble = col
    PyColumn(rcol / v)
  }

}

object PyColumn {
  def fromRDD[T: ClassTag](rdd: RDD[T]) = {
    val tpe = classTag[T]
    if (tpe == classTag[Double]) PyColumn(Column(rdd.sparkContext, rdd.asInstanceOf[RDD[Double]], -1))
    else if (tpe == classTag[String]) PyColumn(Column(rdd.sparkContext, rdd.asInstanceOf[RDD[String]], -1))
    else null
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

object ClassTagUtil {
  val double = classTag[Double]
  val string = classTag[String]
}

object TypeTagUtil {
  val double = ru.typeOf[Double]
  val string = ru.typeOf[String]
}
