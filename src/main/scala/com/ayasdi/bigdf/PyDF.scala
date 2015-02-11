/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author abhishek, ajith, mohit
 *         big dataframe on spark: wrappers for python access via py4j
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

case class PyDF(df: DF) {
    def columnNames = df.columnNames
    def column(name: String) = PyColumn(df.column(name))
    def list = df.list   
    def describe = df.describe
    
    def where(columnName: String, operator: String, value: Double) : PyDF = {
       val filter = operator match {
         case "==" => df(columnName) == value
         
         case "!=" => df(columnName) != value
       }
       PyDF(df.where(filter))
    }
}

case class PyColumn[+T: ru.TypeTag](col: Column[T]) {
    def list(numRows: Int) = col.list(numRows)
}

object PyDF {
  def buildDF(sc: SparkContext, name: String, separator: String, fasterGuess: Boolean): PyDF = {
    val sep : Char = separator.charAt(0)
    PyDF(DF(sc, name, sep, fasterGuess))
  }
}
