/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}
import scala.collection.mutable.HashMap

abstract class Predicate {
  def checkWithRowStrategy(row: Array[Any]): Boolean

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]): Boolean

  def colSeq: List[Int]

  def &(that: Predicate) = &&(that)

  def &&(that: Predicate) = {
    new AndCondition(this, that)
  }

  def |(that: Predicate) = ||(that)

  def ||(that: Predicate) = {
    new OrCondition(this, that)
  }

  def ^^(that: Predicate) = {
    new XorCondition(this, that)
  }

  def unary_~ = unary_!

  def unary_! = {
    new NotCondition(this)
  }
}

abstract class UnaryCondition(val colIndex: Int) extends Predicate with Serializable {
  def colSeq = List(colIndex)
}

abstract class BinaryCondition(val colIndexLeft: Int,
                               val colIndexRight: Int) extends Predicate with Serializable {
  def colSeq = List(colIndexLeft, colIndexRight)
}

abstract class CompoundCondition(val left: Predicate,
                                 val right: Predicate) extends Predicate with Serializable {
  def colSeq = left.colSeq ++ right.colSeq
}

case class DoubleColumnWithDoubleScalarCondition(i: Int, val cmp: Double => Boolean) extends UnaryCondition(i) {
  def checkWithRowStrategy(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[Double])

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    val index = colMap(colIndex)
    cmp(cols(index).asInstanceOf[Double])
  }
}

case class DoubleColumnWithDoubleColumnCondition(i: Int, j: Int, val cmp: (Double, Double) => Boolean)
  extends BinaryCondition(i, j) {
  def checkWithRowStrategy(row: Array[Any]) = {
    val elemLeft = row(colIndexLeft).asInstanceOf[Double]
    val elemRight = row(colIndexRight).asInstanceOf[Double]
    cmp(elemLeft, elemRight)
  }

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    val elemLeft = cols(colMap(colIndexLeft)).asInstanceOf[Double]
    val elemRight = cols(colMap(colIndexRight)).asInstanceOf[Double]
    cmp(elemLeft, elemRight)
  }
}

case class StringColumnWithStringScalarCondition(i: Int,
                                                 val cmp: String => Boolean) extends UnaryCondition(i) {
  def checkWithRowStrategy(row: Array[Any]) = cmp(row(colIndex).asInstanceOf[String])

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    val index = colMap(colIndex)
    cmp(cols(index).asInstanceOf[String])
  }
}

case class StringColumnWithStringColumnCondition(i: Int, j: Int, val cmp: (String, String) => Boolean)
  extends BinaryCondition(i, j) {
  def checkWithRowStrategy(row: Array[Any]) = {
    val elemLeft = row(colIndexLeft).asInstanceOf[String]
    val elemRight = row(colIndexRight).asInstanceOf[String]
    cmp(elemLeft, elemRight)
  }

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    val elemLeft = cols(colMap(colIndexLeft)).asInstanceOf[String]
    val elemRight = cols(colMap(colIndexRight)).asInstanceOf[String]
    cmp(elemLeft, elemRight)
  }
}

case class AndCondition(l: Predicate, r: Predicate) extends CompoundCondition(l, r) {
  def checkWithRowStrategy(row: Array[Any]) = left.checkWithRowStrategy(row) && right.checkWithRowStrategy(row)

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    left.checkWithColStrategy(cols, colMap) && right.checkWithColStrategy(cols, colMap)
  }
}

case class OrCondition(l: Predicate, r: Predicate) extends CompoundCondition(l, r) {
  def checkWithRowStrategy(row: Array[Any]) = left.checkWithRowStrategy(row) || right.checkWithRowStrategy(row)

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) =
    left.checkWithColStrategy(cols, colMap) || right.checkWithColStrategy(cols, colMap)
}

case class XorCondition(l: Predicate, r: Predicate) extends CompoundCondition(l, r) {
  def checkWithRowStrategy(row: Array[Any]) = {
    val a = left.checkWithRowStrategy(row)
    val b = right.checkWithRowStrategy(row)

    (a || b) && !(a && b)
  }

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = {
    val a = left.checkWithColStrategy(cols, colMap)
    val b = right.checkWithColStrategy(cols, colMap)

    (a || b) && !(a && b)
  }
}

case class NotCondition(val cond: Predicate) extends Predicate {
  def checkWithRowStrategy(row: Array[Any]) = !cond.checkWithRowStrategy(row)

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) = !cond.checkWithColStrategy(cols, colMap)

  def colSeq = cond.colSeq
}

case class DoubleColumnCondition(i: Int,
                                 val f: Double => Boolean) extends UnaryCondition(i) {
  def checkWithRowStrategy(row: Array[Any]) = f(row(colIndex).asInstanceOf[Double])

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) =
    f(cols(colMap(colIndex)).asInstanceOf[Double])
}

case class StringColumnCondition(i: Int,
                                 val f: String => Boolean) extends UnaryCondition(i) {
  def checkWithRowStrategy(row: Array[Any]) = f(row(colIndex).asInstanceOf[String])

  def checkWithColStrategy(cols: Array[Any], colMap: HashMap[Int, Int]) =
    f(cols(colMap(colIndex)).asInstanceOf[String])
}



