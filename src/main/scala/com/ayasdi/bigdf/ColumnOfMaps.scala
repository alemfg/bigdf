/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import java.util.{HashSet => JHashSet}

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable

class RichColumnMaps[K, V](self: Column[mutable.Map[K, V]]) {

  def expand(df: DF, keys: Set[String] = null, namePrefix: String = "expanded_"): Unit = {
    require(self.colType == ColType.MapOfStringToFloat) //TODO: support can be added for others

    val ks = Option(keys) getOrElse {
      self.mapOfStringToFloatRdd
        .aggregate(AggDistinctKeys.zeroVal)(AggDistinctKeys.seqOp, AggDistinctKeys.combOp)
    }
    ks.foreach { k =>
      val newColRdd = self.mapOfStringToFloatRdd.map { sparse =>
        sparse.getOrElse(k, 0.0F).toDouble
      }
      newColRdd.name = s"expanded_${k}"
      newColRdd.cache()
      val newCol = Column(self.sc, newColRdd)
      df.setColumn(s"${namePrefix}${k}", newCol)
    }
  }

}

case object AggDistinctKeys {
  def zeroVal: mutable.Set[String] = new JHashSet[String]

  def seqOp(a: mutable.Set[String], b: mutable.Map[String, Float]) = a ++= b.keySet

  def combOp(a: mutable.Set[String], b: mutable.Set[String]) = a ++= b
}

