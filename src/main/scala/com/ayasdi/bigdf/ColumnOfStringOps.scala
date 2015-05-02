/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext


private case object ColumnOfStringsOps {
  def withColumnOfDoubles(sc: SparkContext, a: Column[String], b: Column[Double],
                          oper: (String, Double) => String) = {
    val zipped = a.stringRdd.zip(b.doubleRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def filterDouble(sc: SparkContext, a: Column[String], b: Double,
                   oper: (String, Double) => Boolean) = {
    val result = a.stringRdd.filter { x => oper(x, b)}
    Column(sc, result)
  }

  def withColumnOfString(sc: SparkContext, a: Column[String], b: Column[String],
                         oper: (String, String) => String) = {
    val zipped = a.stringRdd.zip(b.stringRdd)
    val result = zipped.map { x => oper(x._1, x._2)}
    Column(sc, result)
  }

  def withScalarDouble(sc: SparkContext, a: Column[String], b: Double,
                       oper: (String, Double) => String) = {
    val result = a.stringRdd.map { x => oper(x, b)}
    Column(sc, result)
  }

  def withScalarString(sc: SparkContext, a: Column[String], b: String,
                       oper: (String, String) => String) = {
    val result = a.stringRdd.map { x => oper(x, b)}
    Column(sc, result)
  }
}


/*
 * operations with a string as first param
 */
case object StringOps {
  def addDouble(a: String, b: Double) = a + b

  def multiply(a: String, b: Double) = a * b.toInt

  def addString(a: String, b: String) = a + b

  def gt(a: String, b: String) = colBool(a > b)

  def gte(a: String, b: String) = colBool(a >= b)

  private def colBool(bool: Boolean) = {
    if (bool) "t" else "f"
  }

  def lt(a: String, b: String) = colBool(a < b)

  def lte(a: String, b: String) = colBool(a <= b)

  def eq(a: String, b: String) = colBool(a == b)

  def neq(a: String, b: String) = colBool(a != b)

  def gtFilter(b: String)(a: String) = a > b

  def gteFilter(b: String)(a: String) = a >= b

  def ltFilter(b: String)(a: String) = a < b

  def lteFilter(b: String)(a: String) = a <= b

  def eqFilter(b: String)(a: String) = a == b

  def neqFilter(b: String)(a: String) = a != b

  def gtColumn(a: String, b: String) = a > b

  def gteColumn(a: String, b: String) = a >= b

  def ltColumn(a: String, b: String) = a < b

  def lteColumn(a: String, b: String) = a <= b

  def eqColumn(a: String, b: String) = a == b

  def neqColumn(a: String, b: String) = a != b
}

class RichColumnString(self: Column[String]) {
  /**
   * mark a string as NA: mutates the string to empty string
   */
  def markNA(naVal: String): Unit = {
    self.rdd = self.stringRdd.map {cell => if (cell == naVal) "" else cell}
  }
  /**
   * replace NA with another string
   */
  def fillNA(value: String): Unit = {
    self.rdd = self.stringRdd.map { cell => if (cell.isEmpty) value else cell}
  }

  private def addNames(idAndName: Seq[(String, Int)]): Unit = {
    idAndName.foreach { case (name, id) =>
      self.catNameToNum(name) = id.toShort
      self.catNumToName(id.toShort) = name
    }
  }

  def str2cat = {
    val uniqNames = self.stringRdd.distinct.collect.sorted
    val namesAndIds = uniqNames.zipWithIndex
    addNames(namesAndIds)

    val name2Id = self.catNameToNum.clone
    val catRdd = self.stringRdd.map {  name2Id(_) }

    Column(self.sc, catRdd, -1)
  }
  /**
   * compare every element in this column with a number
   */
  def >=(that: String) =
      new StringColumnWithStringScalarCondition(self.index, StringOps.gteFilter(that))
  /**
   * compare every element in this column with a number
   */
  def >(that: String) =
      new StringColumnWithStringScalarCondition(self.index, StringOps.gtFilter(that))

  /**
   * compare every element in this column with a number
   */
  def <=(that: String) =
      new StringColumnWithStringScalarCondition(self.index, StringOps.lteFilter(that))

  /**
   * compare every element in this column with a number
   */
  def <(that: String) =
      new StringColumnWithStringScalarCondition(self.index, StringOps.ltFilter(that))

  def +(that: String) = ColumnOfStringsOps.withScalarString(
    self.sc, self.castString, that, StringOps.addString)
  def *(that: String) = ColumnOfStringsOps.withScalarDouble(
    self.sc, self.castString, that.toInt, StringOps.multiply)
}
