/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package com.ayasdi.bigdf

import collection.JavaConversions._
import org.apache.spark.SparkContext
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}
import Preamble._

case class PyDF(df: DF) {
  def columnNames = df.columnNames

  def column(name: String) = PyColumn(df.column(name))

  def list(numRows: Int, numCols: Int) = df.list(numRows, numCols)

  def where(predicate: PyPredicate): PyDF = {
    PyDF(df(predicate.p))
  }
  
  def aggregate(byColumn: String, aggrColumn: String, aggregator: String) : PyDF = {
    val dfAgg = aggregator match {
      case "Mean" => df.aggregate(byColumn, aggrColumn, AggMean)
      case "StrJoin" => df.aggregate(byColumn, aggrColumn, new AggMakeString(","))
      case _ => null
    }
    PyDF(dfAgg)
  }
}

object PyDF {
  def fromCSV(sc: SparkContext, name: String, separator: String, fasterGuess: Boolean): PyDF = {
    val sep: Char = separator.charAt(0)
    PyDF(DF(sc, name, sep, fasterGuess))
  }
}

case class PyColumn[+T: ru.TypeTag](col: Column[T]) {
  def list(numRows: Int) = col.list(numRows)
  override def toString = {
    val name = s"${col.rdd.name}".split('/').last.split('.').head
    s"$name\t${col.colType}"
  }

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
      case "==" => column.col == value
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
      case "==" => column.col == value
      case "!=" => column.col != value
      case "<" => column.col < value
      case "<=" => column.col <= value
      case ">" => column.col > value
      case ">=" => column.col >= value
    }
    PyPredicate(filter)
  }
}


