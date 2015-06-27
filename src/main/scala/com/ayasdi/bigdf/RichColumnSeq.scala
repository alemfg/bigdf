/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

/**
 * Sequence of columns from a DF
 * @param cols  sequence of pairs. Each pair is a column name and the Column
 */
case class RichColumnSeq(val cols: Seq[Column[Any]]) {
  val sc = cols(0).sc

  def describe() {
    cols.foreach { col =>
        println(col.name + ":")
        println(col.toString)
    }
  }


  override def toString() = {
    cols.map { x =>
      "\n" + x.name + ":\n" + x.toString
    }.reduce { (strLeft, strRight) =>
      strLeft + strRight
    }
  }
}
