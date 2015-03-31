/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */

package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}
import scala.collection.immutable.HashMap

/**
 * Extend this class to do aggregations. Implement aggregate method.
 * Optionally override convert and finalize
 * @tparam U type of the cell e.g. Double
 * @tparam V type of intermediate aggregation e.g. Tuple2[Double, Long] as sum and count for calculating mean
 * @tparam W type of final aggregation e.g. mean would be double
 */
trait Aggregator[U, V, W] {
  /**
   * convert a cell in a column(or cells in multiple columns) to a type that will be aggregated
   * e.g. if we want accumulate strings in a list, convert the string in this cell to a list of single string
   * default implementation just typecasts
   * @param cell value in a cell
   * @return converted cell
   */
  def convert(cell: U): V = {
    cell.asInstanceOf[V]
  }

  def mergeValue(a: V, b: U): V = {
    aggregate(a, convert(b))
  }

  def mergeCombiners(x: V, y: V): V = {
    aggregate(x, y)
  }

  /*
   * user supplied aggregator
   */
  def aggregate(p: V, q: V): V

  def finalize(x: V): W = x.asInstanceOf[W]
}

case object AggCount extends Aggregator[Any, Long, Double] {

  /*
      for each column, set count to 1
   */
  override def convert(a: Any) = 1L

  /*
      add running counts
   */
  def aggregate(a: Long, b: Long) = (a + b)
  
  override def finalize(x: Long): Double = x
}

case object AggMean extends Aggregator[Double, Tuple2[Double, Long], Double] {
  type SumNCount = Tuple2[Double, Long]

  /*
      for each column, set sum to cell's value and count to 1
   */
  override def convert(a: Double) = (a.asInstanceOf[Double], 1L)

  /*
      add running sums and counts
   */
  def aggregate(a: SumNCount, b: SumNCount) = (a._1 + b._1, a._2 + b._2)

  /*
      divide sum by count to get mean
   */
  override def finalize(x: SumNCount) = x._1 / x._2
}

class AggString[W] extends Aggregator[String, Array[String], W] {
  override def convert(a: String) = Array(a.asInstanceOf[String])
  def aggregate(a: Array[String], b: Array[String]) = a ++ b
}

case class AggMakeString(val sep: String = ",") extends AggString[String] {
  override def finalize(a: Array[String]) = a.mkString(sep)
}

//case object TF extends AggText[HashMap[String, Double]] {
//  override def finalize(a: Array[String]) = {
//    val wc = a.map{(_, 1)}
//    wc.reduce{ (l,r) =>
//      if(l._1 == r. _1) (l._1, l._2 + r._2)
//      else  ()
//    }
//  }
//}
