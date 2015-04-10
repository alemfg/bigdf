/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         Utils for Schema management
 */

package com.ayasdi.bigdf

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.runtime.{universe => ru}
import scala.util.{Random, Try}

object SchemaUtils {

  /**
   * try to parse a string as a double
   * use Config.NumberParsing._ to handle exceptions
   */
  def parseDouble(str: String): Double = {
    if (str == null || str.isEmpty) Config.NumberParsing.emptyStrReplace.toDouble
    else if (Config.NumberParsing.nans.contains(str)) Config.NumberParsing.nanValue
    else str.toDouble
  }

  /**
   *  try to parse a string as a double, count parse errors
   */
  def parseDouble(parseErrors: Accumulator[Long], str: String): Double = {
    var y = Double.NaN
    try {
      y = parseDouble(str)
    } catch {
      case _: java.lang.NumberFormatException => parseErrors += 1
    }
    y
  }

  /**
   * guess the type of a column by looking at a random sample
   * however, this is slow because spark will cause a large amount(maybe all)
   * of data to be materialized before it can be sampled
   */
  def guessTypeByRandomSampling(sc: SparkContext, col: RDD[String]) = {
    val samples = col.sample(false, 0.01, (new Random).nextLong)
      .union(sc.parallelize(List(col.first)))
    val parseFailCount = samples.filter { str =>
      Try {
        str.toDouble
      }.toOption == None
    }.count

    if (parseFailCount > 0)
      ru.typeOf[String]
    else
      ru.typeOf[Double]
  }

  /**
   * guess the type of a column by looking at the first few rows (for now 5)
   * only materializes the first few rows of first partition, hence faster
   */
  def guessTypeByFirstFew(col: Array[String]) = {
    val samples = col.take(5)
    val parseFailCount = samples.filter { str =>
      Try {
        parseDouble(str)
      }.toOption == None
    }.length

    if (parseFailCount > 0)
      ru.typeOf[String]
    else
      ru.typeOf[Double]
  }

  /**
   * guess the type of a column by looking at the first element
   * in every partition. faster than random sampling method
   */
  def guessType(sc: SparkContext, col: RDD[String]) = {
    val parseErrors = sc.accumulator(0)
    col.foreachPartition { str =>
      try {
        val y = if (str.hasNext) str.next.toDouble else 0
      } catch {
        case _: java.lang.NumberFormatException => parseErrors += 1
      }
    }
    if (parseErrors.value > 0)
      ru.typeOf[String]
    else
      ru.typeOf[Double]
  }

}
