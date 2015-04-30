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
   * ignore the malformed line and continue
   */
  case object Ignore extends EnumVal

  /**
   * stop parsing and abort
   */
  case object Abort extends EnumVal

  /**
   * if fields are missing in a line, fill in the blanks
   */
  case object Fill extends EnumVal

}


/**
 * Options to control parsing of numbers
 * @param emptyStringReplace replace empty string with this string
 * @param nanStrings these strings are NaNs
 * @param nanValue this is the value to use for NaN
 * @param enable make this false to stop attempting to parse numbers i.e. treat them as strings
 */
case class NumberParsingOpts(val emptyStringReplace: String = "NaN",
                             val nanStrings: Set[String] = HashSet("NaN", "NULL", "N/A"),
                             val nanValue: Double = Double.NaN,
                             val enable: Boolean = true)

/**
 * Options to control parsing of strings
 * @param emptyStringReplace replace empty string with this string
 */
case class StringParsingOpts(val emptyStringReplace: String = "")

/**
 * options to handle exceptions while parsing a line
 * @param badLinePolicy abort, ignore line or fill with nulls when a bad line is encountered
 * @param fillValue if line exception policy is to fill in the blanks, use this value to fill
 */
case class LineParsingOpts(val badLinePolicy: LineExceptionPolicy.EnumVal = LineExceptionPolicy.Ignore,
                           val fillValue: String = "")

/**
 * CSV parsing options
 * @param quoteChar fields containing delimiters, other special chars are quoted using this character
 *                  e.g. "this is a comma ,"
 * @param escapeChar if a quote character appears in a field, it is escaped using this
 *                   e.g. "this is a quote \""
 * @param ignoreLeadingWhitespace ignore white space before a field
 * @param ignoreTrailingWhiteSpace ignore white space after a field
 */
case class CSVParsingOpts(val quoteChar: Char = '"',
                          val escapeChar: Char = '\\',
                          val ignoreLeadingWhitespace: Boolean = true,
                          val ignoreTrailingWhiteSpace: Boolean = true)

/**
 *
 * @param fastSamplingSize the number of samples to look at to guess the type of a field in a CSV file
 * @param fastSamplingEnable set to false to use a slower but possibly more accurate guesssing method
 */
case class SchemaGuessingOpts(val fastSamplingSize: Int = 5,
                              val fastSamplingEnable: Boolean = true)

case class PerfTuningOpts(val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,
                          val filterWithRowStrategy: Boolean = false)

/**
 * bigdf configurable options with default values
 * @param numberParsingOpts options to control parsing of numbers
 * @param stringParsingOpts options to control parsing of strings
 * @param lineParsingOpts options to handle exceptions while parsing a line
 * @param csvParsingOpts options to control the CSV parser
 * @param schemaGuessingOpts schema guessing options
 * @param perfTuningOpts options to tune performance
 */
case class Options(val numberParsingOpts: NumberParsingOpts = NumberParsingOpts(),
                   val stringParsingOpts: StringParsingOpts = StringParsingOpts(),
                   val lineParsingOpts: LineParsingOpts = LineParsingOpts(),
                   val csvParsingOpts: CSVParsingOpts = CSVParsingOpts(),
                   val schemaGuessingOpts: SchemaGuessingOpts = SchemaGuessingOpts(),
                   val perfTuningOpts: PerfTuningOpts = PerfTuningOpts())
