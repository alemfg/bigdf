/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         Utils for accessing stored data e.g. CSV files
 */
package com.ayasdi.bigdf

import java.io.StringReader
import com.univocity.parsers.csv._

abstract class BaseCsvParser(fieldSep: Char, ignoreSpace: Boolean) {
  val lineSep = "\n"
  val parser = makeParser(fieldSep, lineSep, ignoreSpace)
  
  private def makeParser(fieldSep: Char, lineSep: String, ignoreSpace: Boolean) = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(fieldSep)
    format.setLineSeparator(lineSep)
    settings.setIgnoreLeadingWhitespaces(ignoreSpace)
    settings.setIgnoreTrailingWhitespaces(ignoreSpace)
    settings.setReadInputOnSeparateThread(false)
    settings.setInputBufferSize(100) //FIXME: tune the size of this buffer
    settings.setMaxColumns(20480)

    new CsvParser(settings)
  }
}

/**
 * Parser for parsing a line at a time. Not efficient for bulk data.
 * @param _fieldSep
 * @param _ignoreSpace
 */
class LineCsvParser(_fieldSep: Char = ',', _ignoreSpace: Boolean = true)
  extends BaseCsvParser(_fieldSep, _ignoreSpace) {
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
 * @param _fieldSep field separator, comma is the default
 * @param _ignoreSpace whether to ignore surrounding whitespace
 * @param split partition number
 */
class BulkCsvParser (iter: Iterator[String], split: Int, _fieldSep: Char = ',', _ignoreSpace: Boolean = true)
  extends BaseCsvParser(_fieldSep, _ignoreSpace) with Iterator[Array[String]] {
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
 * A Reader that "reads" from a sequence of lines. Spark's textFile method removes newlines at end of
 * each line
 * Univocity parser requires a Reader that provides access to the data to be parsed and needs the newlines to 
 * be present
 * @param iter iterator over RDD[String]
 */
class StringIteratorReader(val iter: Iterator[String], val lineSep: String) extends java.io.Reader {
  require(lineSep.length == 1)
  private var next: Long = 0
  private var length: Long = 0
  private var start: Long = 0
  private var str: String = null
  private val lineSepLen = lineSep.length

  private def refill(): Unit = {
    if(length == next) {
      if(iter.hasNext) {
        str = iter.next
        start = length
        length += (str.length + lineSepLen) //allowance for line separator removed by SparkContext.textFile()
      } else {
        str = null
      }
    }
  }

  override def read(): Int = {
    refill()
    if(next >= length) {
      -1
    } else {
      val cur = next - start
      next += 1
      if (cur == str.length)
        '\n'
      else
        str.charAt(cur.toInt)
    }
  }

  def read(cbuf: Array[Char], off: Int, len: Int): Int = {
    refill()
    var n = 0
    if ((off < 0) || (off > cbuf.length) || (len < 0) ||
      ((off + len) > cbuf.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException()
    } else if (len == 0) {
      n = 0
    }
    else {
      if (next >= length) {
        n = -1
      } else {
        n = Math.min(length - next, len).toInt
        if (n == length - next) {
          str.getChars((next - start).toInt, (next - start + n - 1).toInt, cbuf, off)
          cbuf(off + n - lineSepLen) = lineSep.charAt(0)
        } else {
          str.getChars((next - start).toInt, (next - start + n).toInt, cbuf, off)
        }
        next += n
        if (n < len) {
          val m = read(cbuf, off + n, len - n)
          if(m != -1)
            n += m
        }
      }
    }

    n
  }

  override def skip(ns: Long): Long = {
    throw new IllegalArgumentException("Skip not implemented")
  }

  override def ready = {
    refill()
    true
  }

  override def markSupported = false

  override def mark(readAheadLimit: Int): Unit = {
    throw new IllegalArgumentException("Mark not implemented")
  }

  override def reset(): Unit = {
    throw new IllegalArgumentException("Mark and hence reset not implemented")
  }

  def close(): Unit = { }
}
