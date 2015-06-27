/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}
import scala.language.dynamics
import org.apache.spark.sql.{Column => SColumn}

/**
 * Import these implicit conversions to access the Scala DSL
 */
object Implicits {
  import scala.language.implicitConversions

  implicit def columnAnyToRichColumnMap(col: Column[Any]) = new RichColumnMaps(col.castMapStringToFloat)

  implicit def columnSeqToRichColumnSeq(cols: Seq[Column[Any]]) = new RichColumnSeq(cols)

  implicit def dfToRichDF(df: DF) = new RichDF(df)
}

case class RichDF(self: DF) extends Dynamic {
  /**
   * get a column identified by name
   * @param colName name of the column
   */
  def apply(colName: String) = self.column(colName)

  /**
   * get a column with given index
   * @param index column index
   * @return
   */
  def apply(index: Int): Column[Any] = self.column(index)

  /**
   * get multiple columns by name, indices or index ranges
   * e.g. myDF("x", "y")
   * or   myDF(0, 1, 6)
   * or   myDF(0 to 0, 4 to 10, 6 to 1000)
   * @param items Sequence of names, indices or ranges. No mix n match yet
   */
  def apply[T: ru.TypeTag](items: T*): Seq[Column[Any]] = {
    self.columns(items.toSeq)
  }

  /**
   * refer to a column 'c' in DF 'df' as df.c equivalent to df("c")
   */
  def selectDynamic(colName: String) = {
    val col = self.column(colName)
    if (col == null) println(s"$colName does not match any DF API or column name")
    col
  }

  /**
   * filter by a predicate
   */
  def apply(cond: SColumn) = self.where(cond)

  /**
   * update a column, add or replace
   */
  def update(colName: String, that: Column[Any]) = {
     self.setColumn(colName, that)
  }

  /**
   * update a column "c" of DF "df" like df.c = .. equivalent df("c") = ...
   */
  def updateDynamic(colName: String)(that: Column[Any]) = update(colName, that)
}
