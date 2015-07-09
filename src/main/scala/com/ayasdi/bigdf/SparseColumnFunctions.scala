/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import java.util.{HashSet => JHashSet}

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, MapType}

class SparseColumnFunctions[K, V](self: Column) {

  def expand(df: DF, keys: Set[String] = null, namePrefix: String = "expanded_"): Unit = {
    require(self.colType == ColType.MapOfStringToFloat) //TODO: support can be added for others

    val ks = Option(keys) getOrElse {
      self.mapOfStringToFloatRdd
        .aggregate(AggDistinctKeys.zeroVal)(AggDistinctKeys.seqOp, AggDistinctKeys.combOp)
    }

    ks.foreach { k =>
      val sparseToDense = (sparse: Map[String, Float]) => sparse.getOrElse(k, 0.0F)
      val newCol = callUDF(sparseToDense, FloatType, self.df.get.sdf.col(self.name))
      self.df.get.sdf = self.df.get.sdf.withColumn(s"${namePrefix}${k}", newCol)
    }
  }

}

case object AggDistinctKeys {
  def zeroVal: mutable.Set[String] = new JHashSet[String]

  def seqOp(a: mutable.Set[String], b: Map[String, Float]) = a ++= b.keySet

  def combOp(a: mutable.Set[String], b: mutable.Set[String]) = a ++= b
}

