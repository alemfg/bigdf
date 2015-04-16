/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import scala.collection.mutable.HashSet

class RichColumnMaps[K, V](self: Column[Map[K, V]]) {

  def expand(df: DF, keys: Set[String] = null): Unit = {
    require(self.colType == ColType.MapOfStringToFloat)    //TODO: support can be added for others

    val ks = Option(keys) getOrElse {
      self.mapOfStringToFloatRdd
          .aggregate(AggDistinctKeys.zeroVal)(AggDistinctKeys.seqOp, AggDistinctKeys.combOp)
    }
    ks.foreach { k =>
      val newColRdd = self.mapOfStringToFloatRdd.map { sparse =>
         sparse.getOrElse(k, 0.0F).toDouble
      }
      newColRdd.name = s"expanded_${k}"
      val newCol = Column(self.sc, newColRdd)
      df(s"expanded_${k}") = newCol
    }
  }

}

case object AggDistinctKeys {
  def zeroVal = new HashSet[String]

  def seqOp(a: HashSet[String], b: Map[String, Float]) = a ++= b.keySet

  def combOp(a: HashSet[String], b: HashSet[String]) = a ++= b
}

