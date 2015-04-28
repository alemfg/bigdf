/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

import scala.collection.immutable.HashSet

import org.apache.spark.storage.StorageLevel

/**
 * Action to take when malformed lines are found in a CSV File
 */
object LineExceptionPolicy {

  sealed trait EnumVal

  /**
   * drop the malformed line and continue
   */
  case object Drop extends EnumVal

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

case class NumberParsingOpts(val emptyStringReplace: String = "NaN",
                             val nanStrings: Set[String] = HashSet("NaN", "NULL", "N/A"),
                             val nanValue: Double = Double.NaN,
                             val enable: Boolean = true)

case class StringParsingOpts(val emptyStringReplace: String = "")

case class LineParsingOpts(val badLinePolicy: LineExceptionPolicy.EnumVal = LineExceptionPolicy.Drop)

case class CSVParsingOpts(val quoteChar: Char = '"',
                          val escapeChar: Char = '\\')

case class SchemaGuessingOpts(val fastSamplingSize: Int = 5,
                              val fastSamplingEnable: Boolean = true)

case class PerfTuningOpts(val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,
                          val filterWithRowStrategy: Boolean = false)

case class Options(val numberParsingOpts: NumberParsingOpts = NumberParsingOpts(),
                   val stringParsingOpts: StringParsingOpts = StringParsingOpts(),
                   val lineParsingOpts: LineParsingOpts = LineParsingOpts(),
                   val csvParsingOpts: CSVParsingOpts = CSVParsingOpts(),
                   val schemaGuessingOpts: SchemaGuessingOpts = SchemaGuessingOpts(),
                   val perfTuningOpts: PerfTuningOpts = PerfTuningOpts())
