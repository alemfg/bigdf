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
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

import org.apache.spark.rdd.RDD
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

  case object MapOfStringToFloat extends EnumVal

  case object Undefined extends EnumVal

}

/**
 * One column in a DF. It is a collection of elements of the same type. Can be contained in at most one DF. Once
 * assigned to a DF cannot be "moved" to another DF. To do that make a copy using Column.makeCopy
 * @param scol spark sql/catalyst/dataframe column
 * @param index the index at which this Column exists in containing DF. -1 if not in a DF yet.
 * @param name the name of this Column in containing DF
 * @param df the containing DF
 * @tparam T type of the column's elements
 */
class Column[+T: ru.TypeTag] private[bigdf](var scol: SColumn = null,
                                     var index: Int = -1,
                                     var name: String = "anon",
                                     var df: Option[DF] = None) {

  val sc = df.get.sdf.sqlContext.sparkContext

  /**
   * set names for categories
   * FIXME: this should be somewhere else not in Column[T]
   */
  val catNameToNum: mutable.Map[String, Short] = new JHashMap[String, Short]
  val catNumToName: mutable.Map[Short, String] = new JHashMap[Short, String]

  /**
   * count number of elements. although rdd is var not val the number of elements does not change
   */
  lazy val count = {
    require(!df.isEmpty, "Column is not in a DF")
    df.get.rowCount
  }
  val parseErrors = sc.accumulator(0L)

  /*
     what is the column type?
   */
  val tpe = ru.typeOf[T]
  private[bigdf] val isDouble = sqlType == DoubleType
  private[bigdf] val isFloat = sqlType == FloatType
  private[bigdf] val isString = sqlType == StringType
  private[bigdf] val isShort = sqlType == ShortType
  private[bigdf] val isLong = sqlType == LongType
  private[bigdf] val isArrayOfString = sqlType == ArrayType(StringType)
  private[bigdf] val isArrayOfDouble = sqlType == ArrayType(DoubleType)
  private[bigdf] val isMapOfStringToFloat = sqlType == MapType(StringType, FloatType)

  /*
      use this for demux'ing in column type
      always use pattern matching, never if/else
   */
  val colType: ColType.EnumVal = SparkUtil.sqlToColType(sqlType)

  val sqlType: DataType = new SparkColumnFunctions(scol).dataType

  lazy val csvWritable = isDouble || isFloat || isShort || isString || isLong

  def castDouble = {
    require(isDouble)
    this.asInstanceOf[Column[Double]]
  }

  def castString = {
    require(isString)
    this.asInstanceOf[Column[String]]
  }

  def castFloat = {
    require(isFloat)
    this.asInstanceOf[Column[Float]]
  }

  def castShort = {
    require(isShort)
    this.asInstanceOf[Column[Short]]
  }

  def castLong= {
    require(isShort)
    this.asInstanceOf[Column[Long]]
  }

  def castArrayOfString = {
    require(isArrayOfString)
    this.asInstanceOf[Column[Array[String]]]
  }

  def castMapStringToFloat = {
    require(isMapOfStringToFloat)
    this.asInstanceOf[Column[mutable.Map[String, Float]]]
  }

  override def toString = {
    s"scol: ${scol.toString()} index: $index type: $colType"
  }

  /**
   * print brief description of this column, processes the whole column
   */
  def describe(): Unit = {
    val c = if(df.isEmpty) 0 else count
    println(s"\ttype:${colType}\n\tcount:${c}\n\tparseErrors:${parseErrors}")
    if (isDouble) df.get.describe(name)
  }

  /**
   * get the upto max entries in the column as strings
   */
  def head(max: Int): Array[String] = {
    df.get.sdf.select(name).head(max).map(_.toString())
  }

  /**
   * make a clone of this column, the clone does not belong to any DF and has no name
   */
  def makeCopy = new Column[T](scol)

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
    colType match {
      case ColType.Double => doubleRdd.filter(_.isNaN).count
      case ColType.Float => floatRdd.filter(_.isNaN).count
      case ColType.Long => 0L //Long cannot be NaN ?
      case ColType.Short => shortRdd.filter(_ == RichColumnCategory.CATEGORY_NA).count
      case ColType.String => stringRdd.filter(_.isEmpty).count
      case _ => {
        throw new RuntimeException(s"ERROR: No NA defined for column type ${colType}")
      }
    }
  }

  /**
   * get rdd of doubles to use doublerddfunctions etc
   */
  def doubleRdd = getRdd[Double]

  /**
   * get rdd of floats
   */
  def floatRdd = getRdd[Float]

  /**
   * get rdd of strings to do string functions
   */
  def stringRdd = getRdd[String]

  /**
   * get rdd of shorts
   */
  def shortRdd = getRdd[Short]

  /**
   * get rdd of shorts
   */
  def longRdd = getRdd[Long]

  /**
   * get rdd of array of strings to do text analysis
   */
  def arrayOfStringRdd = getRdd[Array[String]]

  /**
   * get rdd of array of doubles
   */
  def arrayOfDoubleRdd = getRdd[Array[Double]]

  /**
   * get rdd of map from string to float for things like tfidf values of terms
   */
  def mapOfStringToFloatRdd = getRdd[mutable.Map[String, Float]]

  /**
   * get the RDD typecast to the given type
   * @tparam R
   * @return RDD of R's. throws exception if the cast is not applicable to this column
   */
  def getRdd[R: ru.TypeTag] = {
    require(ru.typeOf[R] =:= ru.typeOf[T], s"s${ru.typeOf[R]} does not match s${ru.typeOf[T]}")
    require(!df.isEmpty)
    df.get.sdf.select(name).rdd.map(_(0)).asInstanceOf[RDD[R]]
  }

  /**
   * add two columns
   */
  def +(that: Column[_]) = new Column(scol + that.scol)

  /**
   * subtract a column from another
   */
  def -(that: Column[_]) =  new Column(scol - that.scol)

  /**
   * divide a column by another
   */
  def /(that: Column[_]) =  new Column(scol / that.scol)

  /**
   * multiply a column with another
   */
  def *(that: Column[_]) =  new Column(scol * that.scol)

  def ===(that: Column[_]) = scol === that.scol

  def ===(that: Double) = scol === that

  def !==(that: Column[_]) = scol !== that.scol

  def !==(that: Double) = scol !== that

  def >=(that: Double) = scol >= that

  def <(that: Double) = scol < that

  def >(that: Double) = scol > that

  def <=(that: Double) = scol <= that

  def ===(that: String) = scol === that

  def !==(that: String) = scol !== that

  def >=(that: String) = scol >= that

  def <(that: String) = scol < that

  def >(that: String) = scol > that

  def <=(that: String) = scol <= that

  /**
   * apply a given function to a column to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[U: ru.TypeTag, V: ru.TypeTag](mapper: U => V) = {
    require(ru.typeOf[U] =:= tpe)
    val sdf = df.get.sdf.select(callUDF(mapper, SparkUtil.typeTagToSql(ru.typeOf[V]), df.get.sdf(name)))
    sdf.col("*")
  }

  def dbl_map[U: ClassTag](mapper: Double => U) = {
    val mapped = if (isDouble) {
      doubleRdd.map { row => mapper(row) }
    } else null
    if (classTag[U] == classTag[Double])
      Column(mapped.asInstanceOf[RDD[Double]])
    else if (classTag[U] == classTag[String])
      Column(mapped.asInstanceOf[RDD[String]])
    else null
  }

  def str_map[U: ClassTag](mapper: String => U) = {
    val mapped = if (isString) {
      stringRdd.map { row => mapper(row) }
    } else null
    if (classTag[U] == classTag[Double])
      Column(mapped.asInstanceOf[RDD[Double]])
    else if (classTag[U] == classTag[String])
      Column(mapped.asInstanceOf[RDD[String]])
    else null
  }

}

object Column {
  /**
   * create Column from existing RDD
   */
  def apply[T: ru.TypeTag](rdd: RDD[T], index: Int = -1, name: String = s"fromRdd") = {
    val tpe = ru.typeOf[T]
    val sqlContext = new SQLContext(rdd.sparkContext)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val rows = rdd.map(Row(_))
    val sdf = sqlContext.createDataFrame(rows, StructType(List(StructField(name, SparkUtil.typeTagToSql(tpe)))))
    new Column[T](sdf.col(name), index, name, None)
  }
}
