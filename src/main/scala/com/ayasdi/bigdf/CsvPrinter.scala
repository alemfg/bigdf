/*
 * Copyright 2015 Ayasdi Inc
 */

package com.ayasdi.bigdf.readers

import java.io.StringReader
import com.univocity.parsers.csv._

abstract class CsvPrinter(fieldSep: Char = ',',
                         quote: Char = '"',
                         escape: Char = '\\',
                         ignoreLeadingSpace: Boolean = true,
                         ignoreTrailingSpace: Boolean = true,
                         headers: Seq[String],
                         maxCols: Int = 20480) {
  lazy val writer = {
    val settings = new CsvWriterSettings()
    val format = settings.getFormat
    format.setDelimiter(fieldSep)
    format.setQuote(quote)
    format.setQuoteEscape(escape)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingSpace)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingSpace)
    settings.setMaxColumns(maxCols)
    settings.setNullValue("")
    if(headers != null) settings.setHeaders(headers:_*)

    new CsvWriter(null, settings)
  }
}

/**
 * Parser for parsing a line at a time. Not efficient for bulk data.
 */
class LineCsvWriter(fieldSep: Char = ',',
                    lineSep: String = "\n",
                    quote: Char = '"',
                    escape: Char = '\\',
                    ignoreLeadingSpace: Boolean = true,
                    ignoreTrailingSpace: Boolean = true,
                    inputBufSize: Int = 128,
                    maxCols: Int = 20480)
  extends CsvReader(fieldSep,
    lineSep,
    quote,
    escape,
    ignoreLeadingSpace,
    ignoreTrailingSpace,
    null,
    inputBufSize,
    maxCols) {
  /**
   * parse a line
   * @param line a String with no newline at the end
   * @return array of strings where each string is a field in the CSV record
   */
  def parseLine(line: String): Array[String] = {
    parser.beginParsing(new StringReader(line))
    val parsed = parser.parseNext()
    parser.stopParsing()
    parsed
  }
}

/**
 * Parser for parsing lines in bulk. Use this when efficiency is desired.
 * @param iter iterator over lines in the file
 */
class BulkCsvWriter(iter: Iterator[String],
                     split: Int,      // for debugging
                     fieldSep: Char = ',',
                     lineSep: String = "\n",
                     quote: Char = '"',
                     escape: Char = '\\',
                     ignoreLeadingSpace: Boolean = true,
                     ignoreTrailingSpace: Boolean = true,
                     headers: Seq[String] = null,
                     inputBufSize: Int = 128,
                     maxCols: Int = 20480)
  extends CsvReader(fieldSep,
    lineSep,
    quote,
    escape,
    ignoreLeadingSpace,
    ignoreTrailingSpace,
    headers,
    inputBufSize,
    maxCols)
  with Iterator[Array[String]] {

  val reader = new StringIteratorReader(iter, lineSep)
  parser.beginParsing(reader)
  var nextRecord =  parser.parseNext()

  /**
   * get the next parsed line.
   * @return array of strings where each string is a field in the CSV record
   */
  def next = {
    val curRecord = nextRecord
    if(curRecord != null)
      nextRecord = parser.parseNext()
    else
      throw new NoSuchElementException("next record is null")
    curRecord
  }

  def hasNext = nextRecord != null

}

/**
 * A Writer that...
 */
//class StringIteratorWriter(val iter: Iterator[String], val lineSep: String) extends java.io.Writer {
//  require(lineSep.length == 1)
//  private var next: Long = 0
//  private var length: Long = 0
//  private var start: Long = 0
//  private var str: String = null
//  private val lineSepLen = lineSep.length
//
//  private def refill(): Unit = {
//    if(length == next) {
//      if(iter.hasNext) {
//        str = iter.next
//        start = length
//        length += (str.length + lineSepLen) //allowance for line separator removed by SparkContext.textFile()
//      } else {
//        str = null
//      }
//    }
//  }
//
//  override def write(c: Int): Unit = {
//
//  }
//
//  def write(cbuf: Array[Char], off: Int, len: Int): Int = {
//
//  }
//
//  override def skip(ns: Long): Long = {
//    throw new IllegalArgumentException("Skip not implemented")
//  }
//
//  override def ready = {
//    refill()
//    true
//  }
//
//  override def markSupported = false
//
//  override def mark(readAheadLimit: Int): Unit = {
//    throw new IllegalArgumentException("Mark not implemented")
//  }
//
//  override def reset(): Unit = {
//    throw new IllegalArgumentException("Mark and hence reset not implemented")
//  }
//
//  def close(): Unit = { }
//}
