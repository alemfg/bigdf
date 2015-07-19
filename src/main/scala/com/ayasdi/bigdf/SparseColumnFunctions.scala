/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import java.util.{HashMap => JHashMap, HashSet => JHashSet}

import scala.collection.JavaConversions.{asScalaSet, mapAsScalaMap}
import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType}

class SparseColumnFunctions(self: Column) {

  private def stringToInt(ks: mutable.Set[String]): mutable.Map[String, Int] = {
    val size = ks.size
    println(s"expanding ${self.name} to $size columns")
    val stringToInt = new JHashMap[String, Int]()
    var i = 0
    ks.foreach { k =>
      stringToInt(k) = i
      i += 1
    }
    stringToInt
  }

  def expand(keys: mutable.Set[String] = null, namePrefix: String = "expanded_"): Unit = {
    require(self.colType == ColType.MapOfStringToFloat || self.colType == ColType.MapOfStringToLong)

    val ks: mutable.Set[String] = Option(keys) getOrElse {
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

    self.df.get.stringToIntMaps(s"_e_${self.name}") = stringToInt(ks)

    val newCol = self.colType match {
      case ColType.MapOfStringToFloat =>
        val sparseToDense = (sparse: Map[String, Float]) => {
          val data = new Array[Float](ks.size)
          var i = 0
          ks.foreach { k =>
            data(i) = sparse.getOrElse(k, 0.0F)
            i += 1
          }
          data
        }
        callUDF(sparseToDense, ArrayType(FloatType), self.df.get.sdf.col(self.name))
      case ColType.MapOfStringToLong =>
        val sparseToDense = (sparse: Map[String, Long]) => {
          val data = new Array[Long](ks.size)
          var i = 0
          ks.foreach { k =>
            data(i) = sparse.getOrElse(k, 0L)
            i += 1
          }
          data
        }
        callUDF(sparseToDense, ArrayType(LongType), self.df.get.sdf.col(self.name))
      case _ => throw new IllegalStateException("can't get here")
    }
    val colName = s"_e_${self.name}"
    self.df.get.sdf = self.df.get.sdf.withColumn(colName, newCol)
  }
}

case class AggDistinctKeys[T: ru.TypeTag] {
  def zeroVal: mutable.Set[String] = new JHashSet[String]

  def seqOp(a: mutable.Set[String], b: Map[String, T]) = a ++= b.keySet

  def combOp(a: mutable.Set[String], b: mutable.Set[String]) = a ++= b
}

