/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         Column in a dataframe. It stores the data for the column in an RDD.
 *         A column can exist without being in a dataframe but usually it will be added to one
 */
package com.ayasdi.bigdf

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column => SColumn, Row, SQLContext, SparkColumnFunctions}

/*
  Instead of using the typetag we use this "Enum" to make the compiler generate a
  warning when we add a new column type but forget to handle it somewhere
  To get this warning pattern matching is necessary, ABSOLUTELY NO if/else for column type
 */
object ColType {

  sealed trait EnumVal

  case object Double extends EnumVal

  case object Float extends EnumVal

  case object Short extends EnumVal

  case object Long extends EnumVal

  case object String extends EnumVal

  case object ArrayOfString extends EnumVal

  case object ArrayOfDouble extends EnumVal

  /**
   * Often used to compress multiple logical columns into a single physical column
   */
  case object MapOfStringToFloat extends EnumVal

  case object MapOfStringToLong extends EnumVal

  case object Undefined extends EnumVal

}

/**
 * One column in a DF. It is a collection of elements of the same type. Can be contained in at most one DF. Once
 * assigned to a DF cannot be "moved" to another DF.
 * @param scol spark sql/catalyst/dataframe column
 * @param index the index at which this Column exists in containing DF. -1 if not in a DF yet.
 * @param name the name of this Column in containing DF
 * @param df the containing DF
 */
class Column private[bigdf](var scol: SColumn,
                            var index: Int = -1,
                            var name: String = "anon",
                            var df: Option[DF] = None) {

  /**
   * set names for categories
   * FIXME: this should be somewhere else not in Column
   */
  val catNameToNum: mutable.Map[String, Short] = new JHashMap[String, Short]
  val catNumToName: mutable.Map[Short, String] = new JHashMap[Short, String]

  /**
   * count number of elements.
   */
  def count = {
    require(!df.isEmpty, "Column is not in a DF")
    df.get.rowCount
  }
  val parseErrors = 0 //FIXME: sc.accumulator(0L)

  /*
      use this for demux'ing in column type
      always use pattern matching, never if/else
   */
  lazy val sqlType: DataType = new SparkColumnFunctions(scol).dataType
  lazy val colType: ColType.EnumVal = SparkUtil.sqlToColType(sqlType)

  private[bigdf] def isDouble = sqlType == DoubleType
  private[bigdf] def isFloat = sqlType == FloatType
  private[bigdf] def isString = sqlType == StringType
  private[bigdf] def isShort = sqlType == ShortType
  private[bigdf] def isLong = sqlType == LongType
  private[bigdf] def isArrayOfString = sqlType == ArrayType(StringType)
  private[bigdf] def isArrayOfDouble = sqlType == ArrayType(DoubleType)
  private[bigdf] def isMapOfStringToFloat = sqlType == MapType(StringType, FloatType)

  lazy val csvWritable = isDouble || isFloat || isShort || isString || isLong

  override def toString = {
    s"scol: ${scol.toString()} index: $index type: $colType"
  }

  override def equals(that: Any): Boolean = that match {
    case that: Column => that.scol.equals(this.scol)
    case _ => false
  }

  /**
   * get the upto max entries in the column as strings
   */
  def head(max: Int): Array[String] = {
    df.get.sdf.select(name).head(max).map(_.toString())
  }

  /**
   * print upto max(default 10) elements
   */
  def list(max: Int = 10): Unit = head(max).foreach(println)

  /**
   * does the column have any NA
   */
  def hasNA = countNA > 0

  /**
   * count the number of NAs
   */
  def countNA = {
    require(df.nonEmpty)
    df.get.sdf.filter(scol.isNull).count
  }

  /**
   * get column as rdd of doubles to use doublerddfunctions etc
   */
  def doubleRdd = getRdd[Double]

  /**
   * get column as rdd of floats
   */
  def floatRdd = getRdd[Float]

  /**
   * get column as rdd of strings to do string functions
   */
  def stringRdd = getRdd[String]

  /**
   * get column as rdd of shorts
   */
  def shortRdd = getRdd[Short]

  /**
   * get column as rdd of shorts
   */
  def longRdd = getRdd[Long]

  /**
   * get column as rdd of array of strings to do text analysis
   */
  def arrayOfStringRdd = getRdd[ArrayBuffer[String]]

  /**
   * get column as rdd of array of doubles
   */
  def arrayOfDoubleRdd = getRdd[ArrayBuffer[Double]]

  /**
   * get column as rdd of array of doubles
   */
  def arrayOfFloatRdd = getRdd[ArrayBuffer[Float]]

  /**
   * get column as rdd of map from string to float for things like tfidf values of terms
   */
  def mapOfStringToFloatRdd = getRdd[Map[String, Float]]

  /**
   * get the RDD typecast to the given type
   * @tparam R
   * @return RDD of R's. throws exception if the cast is not applicable to this column
   */
  def getRdd[R: ru.TypeTag] = {
    require(SparkUtil.typeTagToSql(ru.typeOf[R]) == sqlType, s"${ru.typeOf[R]} does not match $sqlType")
    require(df.nonEmpty)
    df.get.sdf.select(name).rdd.map(_(0).asInstanceOf[R])(SparkUtil.typeTagToClassTag[R])
  }

  /**
   * add two columns
   */
  def +(that: Column) = new Column(scol + that.scol)

  /**
   * subtract a column from another
   */
  def -(that: Column) = new Column(scol - that.scol)

  /**
   * divide a column by another
   */
  def /(that: Column) = new Column(scol / that.scol)

  /**
   * multiply a column with another
   */
  def *(that: Column) = new Column(scol * that.scol)

  /**
   * add a constant to a column
   */
  def +(that: Any) = new Column(scol + that)

  /**
   * subtract a constant from a column
   */
  def -(that: Any) = new Column(scol - that)

  /**
   * divide a column by a constant
   */
  def /(that: Any) = new Column(scol / that)

  /**
   * multiply a column with a constant
   */
  def *(that: Any) = new Column(scol * that)

  def ===(that: Column) = scol === that.scol

  def ===(that: Any) = scol === that

  def !==(that: Column) = scol !== that.scol

  def !==(that: Any) = scol !== that

  def >=(that: Any) = scol >= that

  def <(that: Any) = scol < that

  def >(that: Any) = scol > that

  def <=(that: Any) = scol <= that

  def first() = df.get.sdf.select(name).first()

  def distinct = df.get.sdf.select(name).distinct

  /**
   * apply a given function to a column to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[U: ru.TypeTag, V: ru.TypeTag](mapper: U => V) = {
    require(SparkUtil.typeTagToSql(ru.typeOf[U]) == sqlType)
    val newCol = callUDF(mapper, SparkUtil.typeTagToSql(ru.typeOf[V]), df.get.sdf(name))
    new Column(newCol)
  }
}

object Column {
  import scala.language.implicitConversions
  implicit def column2SColumn(col: Column): SColumn = col.scol
  implicit def column2DoubleRDD(col: Column): DoubleRDDFunctions = new DoubleRDDFunctions(col.doubleRdd)
  implicit def column2SparkColumnFunctions(col: Column): SparkColumnFunctions = new SparkColumnFunctions(col.scol)
  implicit def column2Expr(col: Column): Expression = new SparkColumnFunctions(col.scol).expr
}
