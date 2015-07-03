/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         Utils for Schema management
 */

package com.ayasdi.bigdf

import scala.reflect.runtime.{universe => ru}
import scala.util.{Random, Try}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{Accumulator, SparkContext}
import com.ayasdi.bigdf.readers.{BulkCsvReader, LineCsvReader}

import com.databricks.spark.csv.{NumberParsingOpts, StringParsingOpts, CSVParsingOpts}

object SchemaUtils {

  /**
   * try to parse a string as a double
   * use Config.NumberParsing._ to handle exceptions
   */
  def parseDouble(str: String, opts: NumberParsingOpts): Double = {
    if (str == null || str.isEmpty) opts.emptyStringReplace.toDouble
    else if (opts.nanStrings.contains(str)) opts.nanValue
    else str.toDouble
  }

  /**
   * try to parse a string as a double, count parse errors
   */
  def parseDouble(parseErrors: Accumulator[Long], str: String, opts: NumberParsingOpts): Double = {
    var y = Double.NaN
    try {
      y = parseDouble(str, opts)
    } catch {
      case _: java.lang.NumberFormatException => parseErrors += 1
    }
    y
  }

  /**
   * guess the type of a column by looking at a random sample
   * however, this is slow because spark will cause a large amount(maybe all)
   * of data to be materialized before it can be sampled
   */
  def guessTypeByRandomSampling(sc: SparkContext, col: RDD[String]) = {
    val samples = col.sample(false, 0.01, (new Random).nextLong)
      .union(sc.parallelize(List(col.first)))
    val parseFailCount = samples.filter { str =>
      Try {
        str.toDouble
      }.toOption == None
    }.count

    if (parseFailCount > 0)
      ColType.String
    else
      ColType.Double
  }

  /**
   * guess the type of a column by looking at the first few rows (for now 5)
   * only materializes the first few rows of first partition, hence faster
   */
  def guessTypeByFirstFew(samples: Array[String], opts: NumberParsingOpts): ColType.EnumVal = {
    val parseFailCount = samples.filter { str =>
      Try {
        parseDouble(str, opts)
      }.toOption == None
    }.length

    if (parseFailCount > 0)
      ColType.String
    else
      ColType.Double
  }

  /**
   * guess the type of a column by looking at the first element
   * in every partition. faster than random sampling method
   */
  def guessType(sc: SparkContext, col: RDD[String]): ColType.EnumVal = {
    val parseErrors = sc.accumulator(0)
    col.foreachPartition { str =>
      try {
        val y = if (str.hasNext) str.next.toDouble else 0
      } catch {
        case _: java.lang.NumberFormatException => parseErrors += 1
      }
    }
    if (parseErrors.value > 0)
      ColType.String
    else
      ColType.String
  }

  def inferSchema(sc: SparkContext,
                  fileName: String,
                  forceSchema: Map[String, ColType.EnumVal],
                  options: Options): StructType = {
    val file = sc.textFile(fileName)
    // parse header line
    val firstLine = file.first
    val header = new LineCsvReader(fieldSep = options.csvParsingOpts.delimiter,
      ignoreLeadingSpace = options.csvParsingOpts.ignoreLeadingWhitespace,
      ignoreTrailingSpace = options.csvParsingOpts.ignoreTrailingWhitespace,
      quote = options.csvParsingOpts.quoteChar,
      escape = options.csvParsingOpts.escapeChar
    ).parseLine(firstLine)
    println(s"Found ${header.size} columns in header")

    val dataLines = file.mapPartitionsWithIndex({
      case (partitionIndex, iter) => if (partitionIndex == 0) iter.drop(1) else iter
    }, true)

    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        new BulkCsvReader(iter, split,
          fieldSep = options.csvParsingOpts.delimiter,
          ignoreLeadingSpace = options.csvParsingOpts.ignoreLeadingWhitespace,
          ignoreTrailingSpace = options.csvParsingOpts.ignoreTrailingWhitespace,
          quote = options.csvParsingOpts.quoteChar,
          escape = options.csvParsingOpts.escapeChar,
          numFields = header.size,
          badLinePolicy = options.lineParsingOpts.badLinePolicy,
          fillValue = options.lineParsingOpts.fillValue)
      }
    }, true)

    val colName2Type = (for (i <- 0 until header.length) yield {
      val colType = if(forceSchema.contains(header(i))) {
        forceSchema(header(i))
      } else {
        val colStrRdd = rows.map { row => row(i) }
        if (options.schemaGuessingOpts.fastSamplingEnable) {
          val firstFewValues = colStrRdd.take(options.schemaGuessingOpts.fastSamplingSize)
          SchemaUtils.guessTypeByFirstFew(firstFewValues, options.numberParsingOpts)
        } else {
          SchemaUtils.guessType(sc, colStrRdd)
        }
      }

      header(i) -> colType
    }).toMap

    println(s"Inferred Schema: $colName2Type")

    val structFields = header.map { field => StructField(field, SparkUtil.colTypeToSql(colName2Type(field))) }

    StructType(structFields)
  }

}
