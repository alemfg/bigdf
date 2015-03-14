/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package org.apache.spark

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import net.razorvine.pickle.Pickler
 
  

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
}