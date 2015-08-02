/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         Column in a dataframe. It stores the data for the column in an RDD.
 *         A column can exist without being in a dataframe but usually it will be added to one
 */
package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}

import org.apache.spark.rdd.RDD

/**
 * Contains an RDD to be used as a Column in a DF.
 * @param rdd the rdd
 * @param index index of this column in DF
 * @param name name of this column in DF
 * @tparam T
 */
class RDDColumn[T: ru.TypeTag](var rdd: RDD[T],
                                var index: Int = -1,
                                var name: String)

