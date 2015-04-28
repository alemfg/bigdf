/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

import org.apache.spark.storage.StorageLevel._

/**
 * Action to take when malformed lines are found in a CSV File
 */
object LineExceptionPolicy {
  sealed trait EnumVal

  /**
   * drop the malformed line and continue
   */
  case object DropMalformed extends EnumVal

  /**
   * stop parsing and abort
   */
  case object Abort extends EnumVal

  /**
   * if fields are missing in a line, pretend that there are enough empty strings
   * at the end of the line to fill all expected fields
   */
  case object AllowMissingFields extends EnumVal
}

/**
 * bigdf configurable params, a global object for easy scripting
 * change it as needed before creating your DFs. don't change it
 * later.
 */
object Config {
  /**
   * params for text/CSV parsing
   */
  object TextParsing {
    /**
     * config params for parsing numbers
     */
    object Number {
      /**
       * replace an empty string by this
       */
      var emptyStrReplace = "NaN"
      /**
       * which strings represent NaN in the data
       */
      var nans = List("NaN", "NULL", "N/A")
      /**
       * which value to use for NaN
       */
      var nanValue = Double.NaN
      /**
       * try to parse numbers
       */
      var enable = true
    }
    object String {
      /**
       * value to substitute for an empty string
       */
      var emptyStringReplace = ""
    }
    object Exception {
      /**
       * how to deal with malformed line
       * FIXME: implement this
       */
      var parseMode = LineExceptionPolicy.DropMalformed
    }
    object ParsingParams {
      var quoteChar = '"'
      var escapeChar = '\\'
    }
  }
  object SchemaGuessing {
    var fastSamplingSize = 5
    var fastSamplingEnable = true
  }

  object PerfParams {
    var storageLevel = MEMORY_ONLY_SER
    var filterWithRowStrategy = false
  }
}
