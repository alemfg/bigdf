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

  implicit def columnAnyToRichColumnMap(col: Column) = new RichColumnMaps(col)

  implicit def columnSeqToRichColumnSeq(cols: Seq[Column]) = new RichColumnSeq(cols)

  implicit def dfToRichDF(df: DF) = new RichDF(df)
}

case class RichDF(self: DF) extends Dynamic {
  /**
   * get a column identified by its name
   * @param colName name of the column
   */
  def apply(colName: String) = self.column(colName)

  /**
   * get a column by its numeric index
   * @param index index of the column
   * @return
   */
  def apply(index: Int): Column = self.column(index)

  /**
   * get multiple columns by name, indices or index ranges
   * e.g. myDF("x", "y")
   * or   myDF(0, 1, 6)
   * or   myDF(0 to 0, 4 to 10, 6 to 1000)
   * @param items Sequence of names, indices or ranges. No mix n match yet
   */
  def apply[T: ru.TypeTag](items: T*): Seq[Column] = {
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
   * @param cond see [[http://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/DataFrame.html
   *             #filter(org.apache.spark.sql.Column)]]
   */
  def apply(cond: SColumn) = self.where(cond)

  /**
   * update a column, add or replace
   */
  def update(colName: String, that: Column) = {
     self.setColumn(colName, that)
  }

  /**
   * update a column "c" of DF "df" like df.c = ... equivalent df("c") = ...
   */
  def updateDynamic(colName: String)(that: Column) = update(colName, that)
}
