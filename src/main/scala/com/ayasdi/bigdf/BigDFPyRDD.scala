/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package org.apache.spark

import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import net.razorvine.pickle.Unpickler

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD


object BigDFPyRDD {
  var initialized = false

  def initialize(): Unit = {
    SerDeUtil.initialize()
    synchronized {
      if (!initialized) {
        initialized = true
      }
    }
  }

  initialize()

  def pythonRDD(rdd: RDD[_]): JavaRDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      initialize() // lets it be called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }

  def javaRDD[T: ClassTag](pyrdd: JavaRDD[Array[Byte]]): JavaRDD[T] = {
    pyrdd.rdd.mapPartitions { iter =>
      initialize()
      val unpickle = new Unpickler
      iter.flatMap { row =>
        val v = unpickle.loads(row)
        v.asInstanceOf[JArrayList[T]].asScala
      }
    }.toJavaRDD()
  }
}
