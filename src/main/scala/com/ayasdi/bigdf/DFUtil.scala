/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.rdd.RDD
import scala.reflect.{ClassTag, classTag}

private[bigdf] object CountHelper {
  def countNaN(row: Array[Any]) = {
    var ret = 0
    for (col <- row) {
      val a = col match {
        case x: Double => x.isNaN
        case x: String => x.isEmpty
        case x: Short => x == RichColumnCategory.CATEGORY_NA //short is used for category
        case x: Float => x.isNaN
      }
      if (a == true) ret += 1
    }
    ret
  }
}

/*
 * needed this to work around task serialization failure in spark
 */
private[bigdf] case class PivotHelper(grped: RDD[(Any, Iterable[Array[Any]])],
                                      pivotIndex: Int,
                                      pivotValue: String) {
  def get = {
    grped.map {
      case (k, v) =>
        val vv = v.filter { row =>
          row(pivotIndex) match {
            case cellD: Double => cellD.toString == pivotValue
            case cellS: String => cellS == pivotValue
          }
        }
        (k, vv)
    }

  }
}

private[bigdf] object ColumnZipper {
  /**
   * zip columns to get rows as lists
   * @param df
   * @param indices
   * @return
   */
  def zip(df: DF, indices: Seq[Int]) = {
    val arrays = apply(df, indices)
    arrays.map {
      _.toList
    }
  }

  /**
   * zip columns to get rows as arrays
   * @param df
   * @param indices
   * @return RDD of columns zipped into Arrays
   */
  def apply(df: DF, indices: Seq[Int]): RDD[Array[Any]] = {
    val cols = indices.map { colIndex => df.cols(df.colIndexToName(colIndex))}
    apply(cols)
  }

  /**
   * zip columns to get rows as arrays
   * @param cols
   * @return RDD of columns zipped into Arrays
   */
  def apply(cols: Seq[Column[Any]]): RDD[Array[Any]] = {
    val first = cols.head.rdd
    val rest = cols.tail.map {
      _.rdd
    }

    import org.apache.spark.ZipImplicits._

    RDDtoZipRDDFunctions(first).zip(rest)
  }

  /**
   * zip columns and apply mapper to zipped object
   */
  def zipAndMap[U: ClassTag](cols: Seq[Column[Any]])(mapper: Array[Any] => U): RDD[U] = {
    val first = cols.head.rdd
    val rest = cols.tail.map {
      _.rdd
    }

    import org.apache.spark.ZipImplicits._
    RDDtoZipRDDFunctions(first).zipPartitions(rest, false) { iterSeq: Seq[Iterator[Any]] =>
      val temp = new Array[Any](iterSeq.length)
      new Iterator[U] {
        def hasNext = !iterSeq.exists(!_.hasNext)  //FIXME: catch exception instead and make this faster

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
//  def zipAndForEach[U: ClassTag](cols: Seq[Column[Any]])(rowHandler: Array[Any] => Unit): Unit = {
//    val first = cols.head.rdd
//    val rest = cols.tail.map {
//      _.rdd
//    }
//
//    import org.apache.spark.ZipImplicits._
//    RDDtoZipRDDFunctions(first).zipPartitions(rest, false) { iterSeq: Seq[Iterator[Any]] =>
//      val temp = new Array[Any](iterSeq.length)
//      new Iterator[U] {
//        def hasNext = !iterSeq.exists(!_.hasNext)
//
//        def next = {
//          var i = 0
//          iterSeq.foreach { iter =>
//            temp(i) = iter.next
//            i += 1
//          }
//          rowHandler(temp)
//        }
//      }
//    }
//  }
  
}
