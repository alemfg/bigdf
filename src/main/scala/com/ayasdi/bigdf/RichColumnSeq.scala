/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}

import org.apache.spark.sql.functions._

/**
 * Sequence of columns from a DF
 * @param cols  sequence of pairs. Each pair is a column name and the Column
 */
case class RichColumnSeq(cols: Seq[Column]) {

  lazy val df = cols.head.df.get

  def describe() {
    cols.foreach { col =>
        println(col.name + ":")
        println(col.toString)
    }
  }

  override def toString = {
    cols.map { x =>
      "\n" + x.name + ":\n" + x.toString
    }.reduce { (strLeft, strRight) =>
      strLeft + strRight
    }
  }

  /**
   * apply a given function to a column to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[RT: ru.TypeTag, A1: ru.TypeTag](mapper: A1 => RT) = {
    require(cols.length == 1)
    val newCol = callUDF(mapper, SparkUtil.typeTagToSql(ru.typeOf[RT]), df.sdf(cols(0).name))
    new Column(newCol)
  }

  /**
   * apply a given function to 2 columns to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[RT: ru.TypeTag, A1: ru.TypeTag, A2: ru.TypeTag](mapper: (A1, A2) => RT) = {
    require(cols.length == 2)
    val newCol = callUDF(mapper, SparkUtil.typeTagToSql(ru.typeOf[RT]),
      df.sdf(cols(0).name),
      df.sdf(cols(1).name))
    new Column(newCol)
  }

}
