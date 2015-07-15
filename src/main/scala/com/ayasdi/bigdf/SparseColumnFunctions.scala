/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import java.util.{HashSet => JHashSet}

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, LongType}

class SparseColumnFunctions(self: Column) {

  def expand(df: DF, keys: Set[String] = null, namePrefix: String = "expanded_"): Unit = {
    require(self.colType == ColType.MapOfStringToFloat || self.colType == ColType.MapOfStringToLong)

    val ks = Option(keys) getOrElse {
      self.colType match {
        case ColType.MapOfStringToFloat =>
          val aggtor = AggDistinctKeys[Float]
          self.mapOfStringToFloatRdd.aggregate(aggtor.zeroVal)(aggtor.seqOp, aggtor.combOp)

        case ColType.MapOfStringToLong =>
          val aggtor = AggDistinctKeys[Long]
          self.mapOfStringToLongRdd.aggregate(aggtor.zeroVal)(aggtor.seqOp, aggtor.combOp)

        case _ => throw new IllegalStateException("can't get here")
      }
    }

    ks.foreach { k =>
      val newCol = self.colType match {
        case ColType.MapOfStringToFloat =>
          val sparseToDense = (sparse: Map[String, Float]) => sparse.getOrElse(k, 0.0F)
          callUDF(sparseToDense, FloatType, self.df.get.sdf.col(self.name))
        case ColType.MapOfStringToLong =>
          val sparseToDense = (sparse: Map[String, Long]) => sparse.getOrElse(k, 0L)
          callUDF(sparseToDense, LongType, self.df.get.sdf.col(self.name))
        case _ => throw new IllegalStateException("can't get here")
      }
      val colName = s"${namePrefix}${k.replace(".", "_dot_")}"
      self.df.get.sdf = self.df.get.sdf.withColumn(colName, newCol)
    }
  }

}

case class AggDistinctKeys[T: ru.TypeTag] {
  def zeroVal: mutable.Set[String] = new JHashSet[String]

  def seqOp(a: mutable.Set[String], b: Map[String, T]) = a ++= b.keySet

  def combOp(a: mutable.Set[String], b: mutable.Set[String]) = a ++= b
}

