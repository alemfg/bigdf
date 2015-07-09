/* Ayasdi Inc. Copyright 2014, 2015 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */

package com.ayasdi.bigdf

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types.{LongType, MapType}

/**
 * Aggregates frequencies of grouped items into a Map of item to count
 * e.g. Frequency(a, a, b, c, c, c) => (a -> 2, b -> 1, c -> 3)
 */
case class Frequency(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  override def nullable = true

  override def children: Seq[Expression] = child :: Nil

  override def dataType = MapType(child.dataType, LongType)

  override def toString = s"Frequency($child)"

  override def asPartial = {
    val partialTF2 = Alias(Frequency(child), "PartialFrequency")()
    SplitEvaluation(SparseSum(partialTF2.toAttribute), partialTF2 :: Nil)
  }

  override def newInstance(): FrequencyFunction = FrequencyFunction(child, this)
}

case class FrequencyFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val tfs: mutable.Map[Any, Long] = new JHashMap[Any, Long]()

  override def update(input: Row): Unit = {
    val t = expr.eval(input)
    tfs(t) = tfs.getOrElse(t, 0L) + 1L
  }

  override def eval(input: Row): Any = tfs.toMap
}

/**
 * Sums a set of columns in bigdf sparse format
 */
case class SparseSum(child: Expression) extends AggregateExpression {
  override def nullable = true

  override def children: Seq[Expression] = child :: Nil

  override def dataType = child.dataType

  override def toString = s"SparseSum($child)"

  override def newInstance() = SparseSumFunction(child, this)
}

case class SparseSumFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {

  def this() = this(null, null) // Required for serialization.

  val tfs: mutable.Map[Any, Long] = new JHashMap[Any, Long]()

  override def update(input: Row): Unit = {
    val t = expr.eval(input).asInstanceOf[Map[Any, Long]]
    t.foreach { case (term, count) =>
      tfs(term) = tfs.getOrElse(term, 0L) + count
    }
  }

  override def eval(input: Row): Any = tfs
}
