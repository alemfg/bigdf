/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         configuration that controls things like what to do with nulls etc
 */
package com.ayasdi.bigdf

import com.databricks.spark.csv.{CSVParsingOpts, LineParsingOpts, NumberParsingOpts, StringParsingOpts}

/**
 * Schema inference options
 * @param fastSamplingSize the number of samples to look at to guess the type of a field in a CSV file
 * @param fastSamplingEnable set to false to use a slower but possibly more accurate guesssing method
 */
case class SchemaGuessingOpts(fastSamplingSize: Int = 5,
                              fastSamplingEnable: Boolean = true)

case class PerfTuningOpts(cache: Boolean = true)
/**
 * bigdf configurable options with default values
 * @param numberParsingOpts options to control parsing of numbers
 * @param stringParsingOpts options to control parsing of strings
 * @param lineParsingOpts options to handle exceptions while parsing a line
 * @param csvParsingOpts options to control the CSV parser
 * @param schemaGuessingOpts schema guessing options
 * @param perfTuningOpts options to tune performance
 */
case class Options(numberParsingOpts: NumberParsingOpts = NumberParsingOpts(),
                   stringParsingOpts: StringParsingOpts = StringParsingOpts(),
                   lineParsingOpts: LineParsingOpts = LineParsingOpts(),
                   csvParsingOpts: CSVParsingOpts = CSVParsingOpts(),
                   schemaGuessingOpts: SchemaGuessingOpts = SchemaGuessingOpts(),
                   perfTuningOpts: PerfTuningOpts = PerfTuningOpts())
