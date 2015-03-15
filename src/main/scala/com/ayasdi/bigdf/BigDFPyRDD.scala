/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package org.apache.spark

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import net.razorvine.pickle.{Pickler, Unpickler}
import java.util.{ArrayList => JArrayList}
import scala.collection.JavaConverters._
  

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

	def pythonRDD(rdd: RDD[_]) : JavaRDD[Array[Byte]] = {
     rdd.mapPartitions { iter =>
       initialize()  // lets it be called in executor
       new SerDeUtil.AutoBatchedPickler(iter)
     }   
	}

  def javaRDD(pyrdd: JavaRDD[Array[Byte]]) : JavaRDD[Double] = {
	  pyrdd.rdd.mapPartitions { iter =>
	    initialize()
	    val unpickle = new Unpickler
	    iter.flatMap { row =>
	      val v = unpickle.loads(row)
        v.asInstanceOf[JArrayList[Double]].asScala
	    }
	  }.toJavaRDD()
  }
}