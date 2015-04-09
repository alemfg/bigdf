/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

object Config {
  object NumberParsing {
    var emptyStringOK = false
    var emptyStringValue = Double.NaN
  }
}
