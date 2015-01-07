/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

object Preamble {
  import scala.language.implicitConversions
  implicit def columnDoubleToRichColumnDouble(col: Column[Double]) = new RichColumnDouble(col)
  implicit def columnAnyToRichColumnDouble(col: Column[Any]) = new RichColumnDouble(col.castDouble)

  implicit def columnStringToRichColumnString(col: Column[String]) = new RichColumnString(col)
  implicit def columnAnyToRichColumnString(col: Column[Any]) = new RichColumnString(col.castString)

  implicit def columnShortToRichColumnCategory(col: Column[Short]) = new RichColumnCategory(col.castShort)
  implicit def columnAnyToRichColumnCategory(col: Column[Any]) = new RichColumnCategory(col.castShort)
}

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

  case object String extends EnumVal

  case object ArrayOfString extends EnumVal

  case object ArrayOfDouble extends EnumVal

  case object MapOfStringToFloat extends EnumVal

  case object Undefined extends EnumVal
}


class Column[+T: ru.TypeTag] private(val sc: SparkContext,
                                    var rdd: RDD[Any], /* mutates due to fillNA, markNA */
                                    var index: Int) /* mutates when an orphan column is put in a DF */ {
  /**
   * set names for categories
   * FIXME: this should be somewhere else not in Column[T]
   */
  val catNameToNum = new HashMap[String, Short]
  val catNumToName = new HashMap[Short, String]

  /**
   * count number of elements. although rdd is var not val the number of elements does not change
   */
  lazy val count = rdd.count
  val parseErrors = sc.accumulator(0L)

  /*
     what is the column type?
   */
  val tpe = ru.typeOf[T]
  private val isDouble = tpe =:= ru.typeOf[Double]
  private val isFloat = tpe =:= ru.typeOf[Float]
  private val isString = tpe =:= ru.typeOf[String]
  private val isShort = tpe =:= ru.typeOf[Short]
  private val isArrayOfString = tpe =:= ru.typeOf[Array[String]]
  private val isArrayOfDouble = tpe =:= ru.typeOf[Array[Double]]
  private val isMapOfStringToFloat = tpe =:= ru.typeOf[Map[String, Float]]

  /*
      use this for demux'ing in column type
      always use pattern matching, never if/else
   */
  val colType: ColType.EnumVal = if(isDouble) ColType.Double
      else if(isFloat) ColType.Float
      else if(isShort) ColType.Short
      else if(isString) ColType.String
      else if(isArrayOfString) ColType.ArrayOfString
      else if(isMapOfStringToFloat) ColType.MapOfStringToFloat
      else if(isArrayOfDouble) ColType.ArrayOfDouble
      else ColType.Undefined

  /**
   * Spark uses ClassTag but bigdf uses the more functional TypeTag. This method compares the two.
   * @tparam C classtag to compare
   * @return true if this column type and passed in classtag are the same, false otherwise
   */
  def compareType[C: ClassTag] = {
    if (isDouble) classTag[C] == classTag[Double]
    else if (isFloat) classTag[C] == classTag[Double]
    else if (isString) classTag[C] == classTag[String]
    else if (isShort) classTag[C] == classTag[Short]
    else if (isArrayOfString) classTag[C] == classTag[Array[String]]
    else if (isArrayOfDouble) classTag[C] == classTag[Array[Double]]
    else if (isFloat) classTag[C] == classTag[Float]
    else if (isMapOfStringToFloat) classTag[C] == classTag[Map[String, Float]]
    else false
  }

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

  def castShort= {
    require(isShort)
    this.asInstanceOf[Column[Short]]
  }

  def castArrayOfString = {
    require(isArrayOfString)
    this.asInstanceOf[Column[Array[String]]]
  }

  def castMapStringToFloat = {
    require(isMapOfStringToFloat)
    this.asInstanceOf[Column[Map[String, Float]]]
  }

  override def toString = {
    s"rdd: ${rdd.name} index: $index type: $colType"
  }

  /**
   * print brief description of this column
   */
  def describe(): Unit = {
    import com.ayasdi.bigdf.Preamble._
    val c = if (rdd != null) count else 0
    println(s"\ttype:${colType}\n\tcount:${c}\n\tparseErrors:${parseErrors}")
    if(isDouble) castDouble.printStats
  }

  /**
   * print upto max(default 10) elements
   */
  def list(max: Int = 10): Unit = {
    println("Count: $count")
    colType match {
      case ColType.Double => {
        if (count <= max)
          doubleRdd.collect.foreach { println _ }
        else
          doubleRdd.take(max).foreach { println _ }
      }
      case ColType.String => {
        {
          if (count <= max)
            stringRdd.collect.foreach { println _ }
          else
            stringRdd.take(max).foreach { println _ }
        }
      }
      case _ => {
        if (count <= max)
          rdd.collect.foreach { println _ }
        else
          rdd.take(max).foreach { println _ }
      }
    }
  }

  /**
   * distinct
   */
  def distinct = {
    rdd.distinct
  }

  /**
   * does the column have any NA
   */
  def hasNA = {
    countNA > 0
  }

  /**
   * count the number of NAs
   */
  def countNA = {
    colType match {
      case ColType.Double => doubleRdd.filter { _.isNaN }.count
      case ColType.Float => floatRdd.filter { _.isNaN }.count
      case ColType.Short => shortRdd.filter {  _ == RichColumnCategory.CATEGORY_NA }.count //short is used for categories
      case ColType.String => stringRdd.filter { _.isEmpty }.count
      case _ => {
        println(s"WARNING: No NA defined for column type ${colType}")
        0L
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
  def mapOfStringToFloatRdd = getRdd[Map[String, Float]]

  /**
   * get the RDD typecast to the given type
   * @tparam R
   * @return RDD of R's. throws exception if the cast is not applicable to this column
   */
  def getRdd[R: ru.TypeTag] = {
     require(ru.typeOf[R] =:= ru.typeOf[T])
     rdd.asInstanceOf[RDD[R]]
  }

  /**
   * transform column of doubles to a categorical column (column of shorts)
   */
  def asCategorical = {
    require(isDouble)
    Column.asShorts(sc, doubleRdd, -1, doubleRdd.getStorageLevel)
  }

  /**
   * add two columns
   */
  def +(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.addDouble)
    else if (isString && that.isDouble)
      ColumnOfStringsOps.withColumnOfDoubles(sc, this.castString,
        that.castDouble, StringOps.addDouble)
    else null
  }

  /**
   * subtract a column from another
   */
  def -(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.subtract)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * divide a column by another
   */
  def /(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.divide)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * multiply a column with another
   */
  def *(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.multiply)
        .asInstanceOf[Column[Any]]
    else null
  }

  /**
   * generate a Column of boolean. true if this column is greater than another
   */
  def >>(that: Column[_]) = {
    if (isDouble && that.isDouble)
      ColumnOfDoublesOps.withColumnOfDoubles(sc, this.castDouble,
        that.castDouble, DoubleOps.gt)
    else if (isString && that.isString)
      ColumnOfStringsOps.withColumnOfString(sc, this.castString,
        that.castString, StringOps.gt)
    else null
  }

  /**
   * compare two columns
   */
  def ==(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.eqColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.eqColumn)
    else
      null
  }

  def >(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.gtColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.gtColumn)
    else
      null
  }

  def >=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.gteColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.gteColumn)
    else
      null
  }

  def <(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.ltColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.ltColumn)
    else
      null
  }

  def <=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.lteColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.lteColumn)
    else
      null
  }

  def !=(that: Column[_]): Predicate = {
    if (isDouble && that.isDouble)
      new DoubleColumnWithDoubleColumnCondition(index, that.index, DoubleOps.neqColumn)
    else if (isString && that.isString)
      new StringColumnWithStringColumnCondition(index, that.index, StringOps.neqColumn)
    else
      null
  }

  /**
   * compare every element in this column of doubles with a double
   */
  def ==(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.eqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column of string with a string
   */
  def ==(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.eqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column of doubles with a double
   */
  def !=(that: Double) = {
    if (isDouble)
      new DoubleColumnWithDoubleScalarCondition(index, DoubleOps.neqFilter(that))
    else
      null
  }

  /**
   * compare every element in this column of strings with a string
   */
  def !=(that: String) = {
    if (isString)
      new StringColumnWithStringScalarCondition(index, StringOps.neqFilter(that))
    else
      null
  }

  /**
   * filter using custom function
   */
  def filter(f: Double => Boolean) = {
    if (isDouble)
      new DoubleColumnCondition(index, f)
    else
      null
  }

  def filter(f: String => Boolean) = {
    if (isString)
      new StringColumnCondition(index, f)
    else
      null
  }

  /**
   * apply a given function to a column to generate a new column
   * the new column does not belong to any DF automatically
   */
  def map[U: ClassTag](mapper: Any => U) = {
    val mapped = if (isDouble) {
      doubleRdd.map { row => mapper(row)}
    } else {
      stringRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  def num_map[U: ClassTag](mapper: Double => U) = {
    val mapped = if (isDouble) {
      doubleRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

  def str_map[U: ClassTag](mapper: String => U) = {
    val mapped = if (isString) {
      stringRdd.map { row => mapper(row)}
    }
    if (classTag[U] == classTag[Double])
      Column(sc, mapped.asInstanceOf[RDD[Double]])
    else
      Column(sc, mapped.asInstanceOf[RDD[String]])
  }

}

object Column {
  def asDoubles(sCtx: SparkContext, stringRdd: RDD[String], index: Int, cacheLevel: StorageLevel) = {
    val col = new Column[Double](sCtx, null, index)
    val parseErrors = col.parseErrors

    val doubleRdd = stringRdd.map { x =>
      var y = Double.NaN
      try {
        y = x.toDouble
      } catch {
        case _: java.lang.NumberFormatException => parseErrors += 1
      }
      y
    }
    doubleRdd.setName(s"${stringRdd.name}.toDouble").persist(cacheLevel)
    col.rdd = doubleRdd.asInstanceOf[RDD[Any]]

    col
  }

  def asFloats(sCtx: SparkContext, stringRdd: RDD[String], index: Int, cacheLevel: StorageLevel) = {
    val col = new Column[Float](sCtx, null, index)
    val parseErrors = col.parseErrors

    val floatRdd = stringRdd.map { x =>
      var y = Float.NaN
      try {
        y = x.toFloat
      } catch {
        case _: java.lang.NumberFormatException => parseErrors += 1
      }
      y
    }
    floatRdd.setName(s"${stringRdd.name}.toFloat").persist(cacheLevel)
    col.rdd = floatRdd.asInstanceOf[RDD[Any]]

    col
  }

  def asShorts(sCtx: SparkContext, doubleRdd: RDD[Double], index: Int, cacheLevel: StorageLevel) = {
    val col = new Column[Short](sCtx, null, index)
    val parseErrors = col.parseErrors

    val shortRdd = doubleRdd.map { x =>
      val y = x.toShort
      if(y != x || x.isNaN) parseErrors += 1
      y
    }

    shortRdd.setName(s"${doubleRdd.name}.toShort").persist(cacheLevel)
    col.rdd = shortRdd.asInstanceOf[RDD[Any]]

    col
  }

  /**
   * create Column from existing RDD
   */
  def apply[T: ru.TypeTag](sCtx: SparkContext, rdd: RDD[T], index: Int = -1) = {
    new Column[T](sCtx, rdd.asInstanceOf[RDD[Any]], index)
  }
}
