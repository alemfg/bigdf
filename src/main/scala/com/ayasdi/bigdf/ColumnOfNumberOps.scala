/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.DoubleRDDFunctions
import org.apache.spark.util.StatCounter

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.SparkContext


private[bigdf] abstract class ColumnOfNumberOps[T: ru.TypeTag] {

  def withColumnOfDoubles(sc: SparkContext, a: Column[T], b: Column[Double], oper: (Double, Double) => Double) = {
    val zipped = a.doubleRdd.zip(b.doubleRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def filterDouble(sc: SparkContext, a: Column[T], b: Double, oper: (T, Double) => Boolean) = {
    val result = a.getRdd[T].filter { x => oper(x, b)}
    Column(sc, result)
  }

  def withColumnOfString(sc: SparkContext, a: Column[T], b: Column[String], oper: (T, String) => String) = {
    val zipped = a.getRdd[T].zip(b.stringRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def withScalarDouble(sc: SparkContext, a: Column[T], b: Double, oper: (T, Double) => Double) = {
    val result = a.getRdd[T].map { x => oper(x, b)}
    Column(sc, result)
  }

  def withScalarString(sc: SparkContext, a: Column[T], b: String, oper: (T, String) => String) = {
    val result = a.getRdd[T].map { x => oper(x, b)}
    Column(sc, result)
  }
}

private[bigdf] case object ColumnOfDoublesOps extends ColumnOfNumberOps[Double]

private[bigdf] case object ColumnOfFloatsOps extends ColumnOfNumberOps[Float]

/*
 * operations with a double as first param
 */
case object DoubleOps {
  def addDouble(a: Double, b: Double) = a + b

  def subtract(a: Double, b: Double) = a - b

  def divide(a: Double, b: Double) = a / b

  def multiply(a: Double, b: Double) = a * b

  def addString(a: Double, b: String) = a + b

  def gt(a: Double, b: Double) = colBool(a > b)

  private def colBool(bool: Boolean) = {
    if (bool) 1.0 else 0.0
  }

  def gte(a: Double, b: Double) = colBool(a >= b)

  def lt(a: Double, b: Double) = colBool(a < b)

  def lte(a: Double, b: Double) = colBool(a <= b)

  def eq(a: Double, b: Double) = colBool(a == b)

  def neq(a: Double, b: Double) = colBool(a != b)

  def gtFilter(b: Double)(a: Double) = a > b

  def gteFilter(b: Double)(a: Double) = a >= b

  def ltFilter(b: Double)(a: Double) = a < b

  def lteFilter(b: Double)(a: Double) = a <= b

  def eqFilter(b: Double)(a: Double) = a == b

  def neqFilter(b: Double)(a: Double) = a != b

  def gtColumn(a: Double, b: Double) = a > b

  def gteColumn(a: Double, b: Double) = a >= b

  def ltColumn(a: Double, b: Double) = a < b

  def lteColumn(a: Double, b: Double) = a <= b

  def eqColumn(a: Double, b: Double) = a == b

  def neqColumn(a: Double, b: Double) = a != b
}

/*
 * operations with a float as first param
 */
case object FloatOps {
  def addFloat(a: Float, b: Float) = a + b

  def subtract(a: Float, b: Float) = a - b

  def divide(a: Float, b: Float) = a / b

  def multiply(a: Float, b: Float) = a * b

  def addString(a: Float, b: String) = a + b

  def gt(a: Float, b: Float) = colBool(a > b)

  def gte(a: Float, b: Float) = colBool(a >= b)

  def lt(a: Float, b: Float) = colBool(a < b)

  def lte(a: Float, b: Float) = colBool(a <= b)

  private def colBool(bool: Boolean) = {
    if (bool) 1.0 else 0.0
  }

  def eq(a: Float, b: Float) = colBool(a == b)

  def neq(a: Float, b: Float) = colBool(a != b)

  def gtFilter(b: Float)(a: Float) = a > b

  def gteFilter(b: Float)(a: Float) = a >= b

  def ltFilter(b: Float)(a: Float) = a < b

  def lteFilter(b: Float)(a: Float) = a <= b

  def eqFilter(b: Float)(a: Float) = a == b

  def neqFilter(b: Float)(a: Float) = a != b

  def gtColumn(a: Float, b: Float) = a > b

  def gteColumn(a: Float, b: Float) = a >= b

  def ltColumn(a: Float, b: Float) = a < b

  def lteColumn(a: Float, b: Float) = a <= b

  def eqColumn(a: Float, b: Float) = a == b

  def neqColumn(a: Float, b: Float) = a != b
}


class RichColumnDouble(self: Column[Double]) {
  /**
   * statistical information about this column
   */
  var cachedStats: StatCounter = null

  def printStats(): Unit = {
    println(s"\tmax:${stats.max}\n\tmin:${stats.min}\n\tcount:${stats.count}\n\tsum:${stats.sum}\n")
    println(
      s"\tmean:${stats.mean}\n\tvariance(sample):${stats.sampleVariance}\n\tstddev(sample):${stats.sampleStdev}\n")
    println(s"\tvariance:${stats.variance}\n\tstddev:${stats.stdev}")
  }

  def stats = if (cachedStats != null) cachedStats
  else new DoubleRDDFunctions(self.getRdd[Double]).stats

  /**
   * mark this value as NA
   */
  def markNA(naVal: Double): Unit = {
    cachedStats = null
    self.rdd = self.doubleRdd.map { cell => if (cell == naVal) Double.NaN else cell}
  }

  /**
   * replace NA with another number
   */
  def fillNA(value: Double): Unit = {
    cachedStats = null
    self.rdd = self.doubleRdd.map { cell => if (cell.isNaN) value else cell}
  }

  /**
   * compare every element in this column with a number
   */
  def >=(that: Double) =
    new DoubleColumnWithDoubleScalarCondition(self.index, DoubleOps.gteFilter(that))

  /**
   * compare every element in this column with a number
   */
  def >(that: Double) =
    new DoubleColumnWithDoubleScalarCondition(self.index, DoubleOps.gtFilter(that))

  /**
   * compare every element in this column with a number
   */
  def <=(that: Double) =
    new DoubleColumnWithDoubleScalarCondition(self.index, DoubleOps.lteFilter(that))

  /**
   * compare every element in this column with a number
   */
  def <(that: Double) =
    new DoubleColumnWithDoubleScalarCondition(self.index, DoubleOps.ltFilter(that))

  def +(that: Double) = ColumnOfDoublesOps.withScalarDouble(self.sc, self.castDouble, that, DoubleOps.addDouble)
  def -(that: Double) = ColumnOfDoublesOps.withScalarDouble(self.sc, self.castDouble, that, DoubleOps.subtract)
  def /(that: Double) = ColumnOfDoublesOps.withScalarDouble(self.sc, self.castDouble, that, DoubleOps.divide)
  def *(that: Double) = ColumnOfDoublesOps.withScalarDouble(self.sc, self.castDouble, that, DoubleOps.multiply)

}
