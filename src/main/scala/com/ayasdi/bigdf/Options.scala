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
case class SchemaGuessingOpts(var fastSamplingSize: Int = 5,
                              var fastSamplingEnable: Boolean = true)

case class PerfTuningOpts(var cache: Boolean = true)

case class ParquetOpts(var binaryAsString: Boolean = true)

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


/**
 * builds a [[SchemaGuessingOpts]] instance from "text"
 * schemaGuessingOpts.{fastSamplingEnable, fastSamplingSize} are supported [more to be added]
 */
object SchemaGuessingOpts {
  val prefix = "schemaGuessingOpts."
  val build = SchemaGuessingOpts()

  def apply(opts: Map[String, String]): SchemaGuessingOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("fastSamplingEnable", value: String) => build.fastSamplingEnable = value.toBoolean
        case ("fastSamplingSize", value: String) => build.fastSamplingSize = value.toInt
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[PerfTuningOpts]] instance from "text"
 * perfTuningOptions.{cache} are supported [more to be added]
 */
object PerfTuningOpts {
  val prefix = "perfTuningOpts."
  val build = PerfTuningOpts()

  def apply(opts: Map[String, String]): PerfTuningOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("cache", value: String) => build.cache = value.toBoolean
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

/**
 * builds a [[ParquetOpts]] instance from "text"
 * parquetOpts.{cache} are supported [more to be added]
 */
object ParquetOpts {
  val prefix = "parquetOpts."
  val build = ParquetOpts()

  def apply(opts: Map[String, String]): ParquetOpts = {
    for (opt <- opts if opt._1.startsWith(prefix)) {
      (opt._1.stripPrefix(prefix), opt._2) match {
        case ("binaryAsString", value: String) => build.binaryAsString = value.toBoolean
        case _ => throw new IllegalArgumentException(s"Unknown option $opt")
      }
    }

    build
  }
}

object Options {
  def apply(optMap: Map[String, String]): Options = {
    new Options(realNumberParsingOpts = RealNumberParsingOpts(optMap),
      intNumberParsingOpts = IntNumberParsingOpts(optMap),
      stringParsingOpts = StringParsingOpts(optMap),
      lineParsingOpts = LineParsingOpts(optMap),
      csvParsingOpts = CSVParsingOpts(optMap),
      schemaGuessingOpts = SchemaGuessingOpts(optMap),
      perfTuningOpts = PerfTuningOpts(optMap),
      parquetOpts = ParquetOpts(optMap))
  }
}
