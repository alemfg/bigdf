/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         bigdf
 */
package org.apache.spark.sql

import org.apache.spark.sql.functions._
import com.ayasdi.bigdf.Frequency

/**
 * custom additions to functions included in spark
 */
object MoreFunctions {
  def stddev(col: Column): Column = sqrt(avg(col * col) - (avg(col) * avg(col)))

  def frequency(col: Column): Column = Column(Frequency(col.expr))
}

