/* Ayasdi Inc. Copyright 2015 - all rights reserved. */
/**
 * @author mohit
 *         Utils for accessing stored data e.g. CSV files
 */
package com.ayasdi.bigdf

import com.univocity.parsers.csv._
import java.io.StringReader
import java.io.IOException

class CsvFile(iter: Option[Iterator[String]], fieldSep: Char, lineSep: String, ignoreSpace: Boolean, split: Int)
  extends Iterator[Array[String]] {

  val reader = if(iter.isEmpty)
    new ReusableStringReader("")
  else 
    new RDDStringReader(iter.get)
  
  val parser = getParser(fieldSep, lineSep, ignoreSpace)
  parser.beginParsing(reader)
  var nextRecord: Array[String] =  parser.parseNext()
  
  private def getParser(fieldSep: Char, lineSep: String, ignoreSpace: Boolean) = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(fieldSep)
    format.setLineSeparator(lineSep)
    settings.setIgnoreLeadingWhitespaces(ignoreSpace)
    settings.setIgnoreTrailingWhitespaces(ignoreSpace)
    settings.setReadInputOnSeparateThread(false)
   
    new CsvParser(settings)
  }

  def parseLine(line: String): Array[String] = {
    //    reader.setString(line)
    //
    val parsed = parser.parseNext()
    // parser.stopParsing()
    parsed
  }

  def next = {
    println(s"***${split} / ${nextRecord(0)} ${nextRecord(1)} ${nextRecord(3)} ***")
    val curRecord = nextRecord
    if(curRecord != null) 
      nextRecord = parser.parseNext()
    else
      throw new NoSuchElementException("next record is null")
    if(nextRecord != null) println(s"*** ${nextRecord(3)} ***")
    curRecord
  }
  
  def hasNext = nextRecord != null

  def parseLineSlow(line: String): Array[String] = {
    parser.beginParsing(new StringReader(line))
    val parsed = parser.parseNext()
    parser.stopParsing()
    parsed
  }

}

/**
 * A Reader that can be "reinitialized" with low cost to read from a new string for every line
 * in the data
 * Inspired by java.io.StringReader
 * @param str first line to parse
 */
class ReusableStringReader(var str: String) extends java.io.Reader {
  private var length: Int = str.length
  private var next: Int = 0
  private var mark: Int = 0

  def setString(s: String) = {
    str = s
    length = str.length
    next = 0
    mark = 0
  }
  
  private def ensureOpen(): Unit = {
    if (str == null)
      throw new IOException("Stream closed")
  }

  override def read(): Int = {
    ensureOpen()
    if(next >= length)
      -1
    else {
      val cur = next
      next += 1
      str.charAt(cur)
    }
  }

  def read(cbuf: Array[Char], off: Int, len: Int): Int = {
      ensureOpen()
      if ((off < 0) || (off > cbuf.length) || (len < 0) ||
        ((off + len) > cbuf.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException()
      } else if (len == 0) {
        return 0
      }
      if (next >= length)
        return -1
      val n = Math.min(length - next, len)
      str.getChars(next, next + n, cbuf, off)
      next += n
      return n
  }

  override def skip(ns: Long): Long = {
      ensureOpen()
      if (next >= length)
        return 0
      // Bound skip by beginning and end of the source
      var n = Math.min(length - next, ns).toInt
      n = Math.max(-next, n)
      next += n
      return n
  }
  
  override def ready = {
    ensureOpen()
    true
  }

  override def markSupported = true
  
  override def mark(readAheadLimit: Int): Unit = {
    if(readAheadLimit < 0) throw new IllegalArgumentException("Read-ahead limit < 0")
    ensureOpen()
    mark = next
  }
  
  override def reset(): Unit = {
      ensureOpen()
      next = mark
  }

  def close(): Unit = { }
}

/**
 * A Reader that can be "reinitialized" with low cost to read from a new string for every line
 * in the data
 * Inspired by java.io.StringReader
 * @param iter iterator over RDD[String]
 */
class RDDStringReader(val iter: Iterator[String]) extends java.io.Reader {
  private var next: Long = 0
  private var length: Long = 0
  private var start: Long = 0
  private var str: String = null

  private def refill(): Unit = {
    if(length == next) {
      if(iter.hasNext) {
//        str = iter.next + "\n"
        str = iter.next
        start = length
        length += (str.length + 1) //+ 1 for the missing \n
//        start = length
//        length += str.length
        println(s"*** ${str} start=$start next=$next length=$length")
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
    if (next >= length) {
      n = -1
    } else {
      n = Math.min(length - next, len).toInt
      if (next + n - start == str.length) {
        str.getChars((next - start).toInt, (next + n - start).toInt, cbuf, off)
      } else {
        str.getChars((next - start).toInt, (next + n - start - 1).toInt, cbuf, off)
        cbuf(off + n - 1) = '\n'
      }
      next += n
      var eof = false
      while (n < len && !eof) {
        val m = read(cbuf, off + n, len - n)
        if(m != -1)
          n += m
        else
          eof = true
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
