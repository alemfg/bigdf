/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         fast column zipping for row processing
 */
package com.ayasdi.bigdf

import java.util.{HashMap => JHashMap}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.ZipImplicits._
import org.apache.spark.rdd.RDD

/**
 * Efficient methods to zip columns into rows or partial rows using RDDtoZipRDDFunctions
 * and processing the zipped elements.
 * Efficiency comes from avoiding creation of all intermediate zipped elements, instead a mutable
 * object(one per partition) is used and only the result of desired processing is instantiated in bulk
 */
private[bigdf] object ColumnZipper {

  /**
   * zip columns to get rows as arrays
   * @param cols
   * @return RDD of columns zipped into Arrays
   */
  def makeRows(cols: Seq[RDD[Any]]): RDD[Array[Any]] = {
    val first = cols.head
    val rest = cols.tail

    RDDtoZipRDDFunctions(first).zip(rest)
  }

  /**
   * zip columns and apply mapper to zipped object
   */
  def zipAndMap[U: ClassTag](cols: Seq[RDD[Any]])(mapper: Array[Any] => U): RDD[U] = {
    val first = cols.head
    val rest = cols.tail

    RDDtoZipRDDFunctions(first).zipPartitions(rest, false) { iterSeq: Seq[Iterator[Any]] =>
      val temp = new Array[Any](iterSeq.length)
      new Iterator[U] {
        def hasNext = !iterSeq.exists(!_.hasNext) //FIXME: catch exception instead and make this faster

        def next = {
          var i = 0
          iterSeq.foreach { iter =>
            temp(i) = iter.next
            i += 1
          }
          mapper(temp)
        }
      }
    }
  }

  /**
   * zip columns and apply mapper to zipped object
   */
  def zipAndFilter(cols: Seq[RDD[Any]])(matcher: Array[Any] => Boolean): RDD[Boolean] = {
    val first = cols.head
    val rest = cols.tail

    RDDtoZipRDDFunctions(first).zipPartitions(rest, false) { iterSeq: Seq[Iterator[Any]] =>
      val temp = new Array[Any](iterSeq.length)
      new Iterator[Boolean] {
        var validNextOne = false
        var nextOne = false
        def hasNext = if(!iterSeq.exists(!_.hasNext)) {
          false
        } else {
          while(!nextOne) tryNext
          true
        } //FIXME: catch exception instead and make this faster

        def tryNext = {
          var i = 0
          iterSeq.foreach { iter =>
            temp(i) = iter.next
            i += 1
          }
          nextOne = matcher(temp)
        }

        def next = {
          val thisOne = nextOne
          while(!nextOne) tryNext
          thisOne
        }
      }
    }
  }

  /**
   * filter col by corresponding boolean value in mask
   */
  def filterBy[U: ClassTag](rdd: RDD[U], mask: RDD[Boolean]): RDD[U] = {
    rdd.zipPartitions(mask, false) { case (elemIter, filterIter) =>
      new Iterator[U] {
        private var hd: U = _
        private var hdDefined = false

        def hasNext: Boolean = hdDefined || {
          do {
            if (!elemIter.hasNext) return false
            hd = elemIter.next()
          } while (!filterIter.next())
          hdDefined = true
          true
        }

        def next() = if (hasNext) {
          hdDefined = false;
          hd
        } else throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

}
