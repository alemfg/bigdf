/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

object Config {
  object NumberParsing {
    var emptyStrReplace = "NaN"
    var nans = List("NaN", "NULL", "N/A")
    var nanValue = Double.NaN
    var enable = true
  }
  object SchemaGuessing {
    var fastSamplingSize = 5
  }
}
