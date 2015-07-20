/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi

package object bigdf {

  import scala.language.implicitConversions

  implicit def columnAnyToRichColumnMap(col: Column): SparseColumnSet = new SparseColumnSet(col)

  implicit def columnSeqToRichColumnSeq(cols: Seq[Column]): RichColumnSeq = new RichColumnSeq(cols)

  implicit def dfToRichDF(df: DF): RichDF = new RichDF(df)
}
