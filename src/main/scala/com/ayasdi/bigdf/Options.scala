/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

import com.databricks.spark.csv._

/**
 * Schema inference options
 * @param fastSamplingSize the number of samples to look at to guess the type of a field in a CSV file
 * @param fastSamplingEnable set to false to use a slower but possibly more accurate guesssing method
 */
case class SchemaGuessingOpts(fastSamplingSize: Int = 5,
                              fastSamplingEnable: Boolean = true)

case class PerfTuningOpts(cache: Boolean = true)

case class ParquetOpts(binaryAsString: Boolean = true)

/**
 * bigdf configurable options with default values
 * @param realNumberParsingOpts options to control parsing of numbers
 * @param stringParsingOpts options to control parsing of strings
 * @param lineParsingOpts options to handle exceptions while parsing a line
 * @param csvParsingOpts options to control the CSV parser
 * @param schemaGuessingOpts schema guessing options
 * @param perfTuningOpts options to tune performance
 */
case class Options(realNumberParsingOpts: RealNumberParsingOpts = RealNumberParsingOpts(),
                   intNumberParsingOpts: IntNumberParsingOpts = IntNumberParsingOpts(),
                   stringParsingOpts: StringParsingOpts = StringParsingOpts(),
                   lineParsingOpts: LineParsingOpts = LineParsingOpts(),
                   csvParsingOpts: CSVParsingOpts = CSVParsingOpts(),
                   schemaGuessingOpts: SchemaGuessingOpts = SchemaGuessingOpts(),
                   perfTuningOpts: PerfTuningOpts = PerfTuningOpts(),
                   parquetOpts: ParquetOpts = ParquetOpts())
