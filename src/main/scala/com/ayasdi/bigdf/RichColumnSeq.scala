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

  /**
   * apply a function to some columns to produce a new column
   * @param mapper the function to be applied
   * @tparam U  return type of the function
   * @return  a new column
   */
  def slowMap[U: ClassTag](mapper: Array[Any] => U): Column[Any] = {
    val tpe = classTag[U]
    val zippedCols = ColumnZipper.makeRows(cols)
    val mapped = zippedCols.map { row => mapper(row) }
    if (tpe == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else if (tpe == classTag[String])
      Column(sc, mapped.asInstanceOf[RDD[String]])
    else
      null
  }

  /**
   * apply a function to some columns to produce a new column
   * @param mapper the function to be applied
   * @tparam U  return type of the function
   * @return  a new column
   */
  def map[U: ClassTag](mapper: Array[Any] => U): Column[Any] = {
    val tpe = classTag[U]
    val mapped = ColumnZipper.zipAndMap(cols) { row => mapper(row) }
    if (tpe == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else if (tpe == classTag[String])
      Column(sc, mapped.asInstanceOf[RDD[String]])
    else
      null
  }

  override def toString() = {
    cols.map { x =>
      "\n" + x.name + ":\n" + x.toString
    }.reduce { (strLeft, strRight) =>
      strLeft + strRight
    }
  }
}
