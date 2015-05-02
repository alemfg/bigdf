/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         Enhancements to RDD.zip API of spark
 */

package org.apache.spark //workaround for private[spark] access of ZippedPartitionsRDD

import org.apache.spark.rdd._
import scala.reflect.ClassTag

object ZipImplicits {
  import scala.language.implicitConversions
  implicit def RDDtoZipRDDFunctions[T: ClassTag](rdd: RDD[T]) = new ZipRDDFunctions(rdd)
}

class ZipRDDFunctions[T](self: RDD[T])(implicit tpe: ClassTag[T])
  extends Logging
  with Serializable {
  
  /**
   * Zips this RDD with a Seq of other RDDs, returning an RDD of Array[Any]. Each Array
   * contains an element of this RDD followed by elements from other RDDs
   * Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip(others: Seq[RDD[_]]): RDD[Array[Any]] = {
    zipPartitions(others, preservesPartitioning = false) { iterSeq: Seq[Iterator[Any]] =>
      new Iterator[Array[Any]] {
        def hasNext = !iterSeq.exists(! _.hasNext)
        def next = iterSeq.map { iter => iter.next }.toArray
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[V: ClassTag]
  (others: Seq[RDD[_]], preservesPartitioning: Boolean)
  (f: (Seq[Iterator[Any]]) => Iterator[V]): RDD[V] =
    new ZippedPartitionsRDD(self.sparkContext, self.sparkContext.clean(f), this.self +: others, false)

}


class ZippedPartitionsRDD[V: ClassTag](sc: SparkContext,
               f: (Seq[Iterator[_]] => Iterator[V]),
               var rddSeq: Seq[RDD[_]],
               preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, rddSeq, preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdds.zipWithIndex.map (rdd => rdd._1.iterator(partitions(rdd._2), context)))
  }

  override def clearDependencies() {
    super.clearDependencies()
  }
}
