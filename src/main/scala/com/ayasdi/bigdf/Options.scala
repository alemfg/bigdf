/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

import org.apache.spark.storage.StorageLevel
import com.databricks.spark.csv.{CSVParsingOpts, LineParsingOpts, StringParsingOpts, NumberParsingOpts}

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
