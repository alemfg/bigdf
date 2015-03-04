/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *  dataframe on spark
 */

package com.ayasdi.bigdf

import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.TraversableOnce.MonadOps
import Preamble._

class DFTest extends FunSuite with BeforeAndAfterAll {
    implicit var sc: SparkContext = _
    val file: String = "/tmp/test_abcd.csv"

    def fileCleanup: Unit = {
      try {
        Files.deleteIfExists(Paths.get(file))
      } catch {
        case _: Throwable => println("Exception while deleting temp file")
      }
    }

    override def beforeAll: Unit = {
        SparkUtil.silenceSpark
        System.clearProperty("spark.master.port")
        sc = new SparkContext("local[4]", "abcd")
        fileCleanup
    }

    override def afterAll: Unit = {
        fileCleanup
        sc.stop
    }

    private[bigdf] def makeDF = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector(11.0, 12.0, 13.0),
            Vector(21.0, 22.0, 23.0),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private[bigdf] def makeDFWithNAs = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector(Double.NaN, 12.0, 13.0),
            Vector("b1", "", "b3"),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private[bigdf] def makeDFWithNulls = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector(-1, 12.0, 13.0),
            Vector("b1", "NULL", "b3"),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }
    
    private[bigdf] def makeDFWithString = {
        val h = Vector("a", "b", "c", "Date")
        val v = Vector(Vector("11.0", "12.0", "13.0"),
            Vector(21.0, 22.0, 23.0),
            Vector(31.0, 32.0, 33.0),
            Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
        DF(sc, h, v)
    }

    private[bigdf] def makeDFFromCSVFile(file: String) = {
        DF(file, ',', false)
    }

    test("Construct: DF from Vector") {
        val df = makeDF
        assert(df.columnCount === 4)
        assert(df.rowCount === 3)
    }

    test("Construct: DF from CSV file") {
        val df = makeDFFromCSVFile("src/test/resources/pivot.csv")
        assert(df.columnCount === 4)
        assert(df.rowCount === 4)
    }

    test("Column Index: Refer to a column of a DF") {
        val df = makeDF
        val colA = df("a")
        assert(colA.colType === ColType.Double)
        assert(colA.index === 0)
        assert(colA.rdd.count === 3)
        val col0 = df(0)
        assert((col0 eq colA) === true)
    }

    test("Array of column names") {
      val df = makeDF
      assert(df.columnNames ===  Array("a", "b", "c", "Date"))
    }

    test("Column Index: Refer to non-existent column of a DF") {
        val df = makeDF
        val col = df("aa")
        assert(col === null)
    }

    test("Column Index: Refer to multiple columns of a DF") {
        val df = makeDF
        val colSeq = df("a", "b")
        val acol = colSeq.cols(0)
        val bcol = colSeq.cols(1)
        assert(acol._1 === "a")
        assert((acol._2 eq df("a")) === true)
        assert(bcol._1 === "b")
        assert((bcol._2 eq df("b")) === true)
    }

    test("Column Index: Refer to non-existent columns of a DF") {
        val df = makeDF
        val colSeq = df("a", "bb")
        assert(colSeq === null)
    }

    test("Column Index: Slices") {
        val df = makeDF

        val colSeq2 = df(0 to 0, 1 to 3)
        assert(colSeq2.cols.length === 4)
        assert((colSeq2.cols(0)._2 eq df("a")) === true)
        assert((colSeq2.cols(1)._2 eq df("b")) === true)
        assert((colSeq2.cols(2)._2 eq df("c")) === true)
    }

    test("Column Index: Rename") {
        val df = makeDF
        val df2 = df.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"))
        assert((df eq df2) === true)
        assert(df2.colIndexToName(0) === "aa")
        assert(df2.colIndexToName(1) === "bb")
        assert(df2.colIndexToName(2) === "c")

        val df3 = makeDF
        val df4 = df3.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"), false)
        assert((df3 ne df4) === true)
        assert(df4.colIndexToName(0) === "aa")
        assert(df4.colIndexToName(1) === "bb")
        assert(df4.colIndexToName(2) === "c")
    }

    test("Parsing: Parse doubles") {
        val df = makeDFFromCSVFile("src/test/resources/mixedDoubles.csv")
        df.cols.foreach { col =>
          col._2.colType match {
            case ColType.Double => col._2.doubleRdd.collect
            case _ => null
          }
        }
        assert(df("Feature1").parseErrors.value === 1)
    }

    test("Double to Categorical") {
        val df = makeDF
        val nCols = df.columnCount
        df("cat_a") = df("a").asCategorical
        df("cat_a").shortRdd.collect
        assert(df("cat_a").parseErrors.value === 0)
        assert(df.columnCount === nCols + 1)
    }

    test("Double to Categorical: errors") {
        val df = makeDFFromCSVFile("src/test/resources/mixedDoubles.csv")
        df("cat_f1") = df("Feature1").asCategorical
        df("cat_f1").shortRdd.collect
        assert(df("cat_f1").parseErrors.value === 2)
    }

    test("Filter/Select: Double Column comparisons with Scalar") {
        val df = makeDF
        val dfEq12 = df(df("a") == 12)
        assert(dfEq12.rowCount === 1)
        val dfNe12 = df(df("a") != 12.0)
        assert(dfNe12.rowCount === 2)
        val dfGt12 = df(df("a") > 12)
        assert(dfGt12.rowCount === 1)
        val dfGtEq12 = df(df("a") >= 12)
        assert(dfGtEq12.rowCount === 2)
        val dfLt12 = df(df("a") < 12)
        assert(dfLt12.rowCount === 1)
        val dfLtEq12 = df(df("a") <= 12)
        assert(dfLtEq12.rowCount === 2)
    }

    test("Filter/Select: Double Column comparisons with Scalar, no match") {
        val df = makeDF
        val dfGt13 = df(df("a") == 133)
        assert(dfGt13.rowCount === 0)
    }

    test("Filter/Select: String Column comparisons with Scalar") {
        val df = makeDFWithString
        val dfEq12 = df(df("a") == "12.0")
        assert(dfEq12.rowCount === 1)
        val dfGt12 = df(df("a") == "12.0")
        assert(dfGt12.rowCount === 1)
        val dfGtEq12 = df(df("a") >= "12.0")
        assert(dfGtEq12.rowCount === 2)
        val dfLt12 = df(df("a") < "12.0")
        assert(dfLt12.rowCount === 1)
        val dfLtEq12 = df(df("a") <= "12.0")
        assert(dfLtEq12.rowCount === 2)
    }

    test("Filter/Select: String Column comparisons with Scalar, no match") {
        val df = makeDFWithString
        val dfGt13 = df(df("a") > "13.0")
        assert(dfGt13.rowCount === 0)
    }

    test("Filter/Select: Logical combinations of predicates") {
        val df = makeDF
        val dfAeq12AndBeq22 = df(df("a") == 12.0 && df("b") == 22)
        assert(dfAeq12AndBeq22.rowCount === 1)
        val dfAeq12OrBeq23 = df(df("a") == 12 || df("b") == 23)
        assert(dfAeq12OrBeq23.rowCount === 2)
        val dfNotAeq12 = df(!(df("a") == 12))
        assert(dfNotAeq12.rowCount === 2)
        val dfAeq12XorBeq23 = df(df("a") == 12 ^^ df("b") == 23)
        assert(dfAeq12XorBeq23.rowCount === 2)
    }

    test("NA: Counting NaNs") {
        val df = makeDFWithNAs
        assert(df.countColsWithNA === 2)
        assert(df.countRowsWithNA === 2)
    }

    test("NA: Dropping rows with NaN") {
        val df = makeDFWithNAs
        assert(df.rowCount === 3)
        val df2 = df.dropNA(rowStrategy = true)
        assert(df2.rowCount === 1)
    }

    test("NA: Replacing NA with something else") {
        var df = makeDFWithNAs
        assert(df.countRowsWithNA === 2)
        df("a").fillNA(99.0)
        assert(df.countRowsWithNA === 1)
        df("b").fillNA("hi")
        assert(df.countRowsWithNA === 0)

        df = makeDFWithNAs
        df("cat_a") = df("a").asCategorical
        df("cat_a").markNACategory(0)
        df.list()
        assert(df("cat_a").hasNA)
    }
    
    test("NA: Marking a value as NA") {
        val df = makeDFWithNulls
        assert(df.countRowsWithNA === 0)
        df("a").markNA(-1.0)
        assert(df.countRowsWithNA === 1)
        df("b").markNA("NULL")
        assert(df.countRowsWithNA === 2)
    }

    test("Column Ops: New column as simple function of existing ones") {
        val df = makeDF
        val aa = df("a").doubleRdd.first
        val bb = df("b").doubleRdd.first

        df("new") = df("a") + df("b")
        assert(df("new").doubleRdd.first === aa + bb)
        df("new") = df("a") - df("b")
        assert(df("new").doubleRdd.first === aa - bb)
        df("new") = df("a") * df("b")
        assert(df("new").doubleRdd.first === aa * bb)
        df("new") = df("a") / df("b")
        assert(df("new").doubleRdd.first === aa / bb)
    }

    test("Column Ops: New column as simple function of existing column and scalar") {
      var df = makeDF
      val aa = df("a").doubleRdd.first

      df("new") = df("a") + 2
      assert(df("new").doubleRdd.first === aa + 2)
      df("new") = df("a") - 2
      assert(df("new").doubleRdd.first === aa - 2)
      df("new") = df("a") * 2
      assert(df("new").doubleRdd.first === aa * 2)
      df("new") = df("a") / 2
      assert(df("new").doubleRdd.first === aa / 2)

      df = makeDFWithString
      val aaa = df("a").stringRdd.first
      df("new") = df("a") + "2"
      assert(df("new").stringRdd.first === aaa + 2)
      df("new") = df("a") * "2"
      assert(df("new").stringRdd.first === aaa * 2)
    }

    test("Column Ops: New column as custom function of existing ones") {
        val df = makeDF
        df("new") = df("a", "b").map(TestFunctions.summer)
        assert(df("new").doubleRdd.first === 21 + 11)
    }

    test("Column Ops: New column as custom function of existing ones - faster?") {
        val df = makeDF
        df("new") = df("a", "b").map2(TestFunctions2.summer)
        assert(df("new").doubleRdd.first === 21 + 11)
    }

    test("Aggregate") {
        val df = makeDF
        df("groupByThis") = df("a").num_map { x => 1.0 }
        val sumOfA = df.aggregate("groupByThis", "a", AggSimple)
        assert(sumOfA("a").doubleRdd.first === df("a").doubleRdd.sum)
        val arrOfA = df.aggregate("groupByThis", "a", AggCustom)
        assert(arrOfA("a").doubleRdd.first === df("a").doubleRdd.sum)
    }

    test ("Aggregate multi") {
      val df = makeDFFromCSVFile("src/test/resources/aggregate.csv")
      val sumOfFeature1 = df.aggregate(List("Month", "Customer"), List("Feature1"), AggSimple)
      assert(sumOfFeature1.columnCount === 3)
      assert(sumOfFeature1.rowCount === 4)
    }

    test ("Aggregate string") {
      val df = makeDFWithString
      df("groupByThis") = df("a").str_map( x => "hey")
      val arrOfA = df.aggregate("groupByThis", "a", new AggMakeString(sep=";"))
      val strOfA = arrOfA("a").stringRdd.first
      assert(strOfA.contains("11.0") && strOfA.contains("12.0") && strOfA.contains("13.0"))
    }

    test("Pivot") {
        val df = makeDFFromCSVFile("src/test/resources/pivot.csv")
        df.list()
        val df2 = df.pivot("Customer", "Period")
        df2.describe
        df2.list()
        assert(df2.rowCount === 3)
        assert(df2.columnCount === 6)
        val df3 = df2("S_Customer@Period==2.0").stringRdd.zip(df2("S_Customer@Period==1.0").stringRdd)
        val bad = sc.accumulator(0)
        df3.foreach { case (a, b) => 
            if(!a.isEmpty && !b.isEmpty && a != b)
                bad += 1
        }
        assert(bad.value === 0)
    }

    test("toCSV") {
      val df = makeDF
      val csvRows = df.toCSV().collect()
      assert(csvRows(0) === "a,b,c,Date")
      assert(csvRows(1) === "11.0,21.0,31.0,1.36074391383E12")
      assert(csvRows(2) === "12.0,22.0,32.0,1.360616948975E12")
      assert(csvRows(3) === "13.0,23.0,33.0,1.36055080601E12")
    }

    test("toParquet") {
      val df = makeDF
      df.writeToParquet("/tmp/x")
    }

}

class DFTestWithKryo extends DFTest {
    override def beforeAll {
        SparkUtil.silenceSpark
        System.clearProperty("spark.master.port")

        var conf = new SparkConf()
            .setMaster("local[4]")
            .setAppName("DFTestWithKryo")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        sc = new SparkContext(conf)
    }
}

case object AggSimple extends Aggregator[Double, Double, Double] {
    def aggregate(a: Double, b: Double) = a + b
}

case object AggCustom extends Aggregator[Double, Array[Double], Double] {
    override def convert(a: Double): Array[Double] = { Array(a.asInstanceOf[Double]) }
    def aggregate(a: Array[Double], b: Array[Double]) = a ++ b
    override def finalize(x: Array[Double]) = x.sum
}

case object TestFunctions {
    def summer(cols: Array[Any]) = {
        (cols(0), cols(1)) match {
            case (a: Double, b: Double) => a + b
        }
    }
}

case object TestFunctions2 {
    def summer(cols: Array[Any]) = {
        println(cols(0), cols(1))
        cols(0).asInstanceOf[Double] + cols(1).asInstanceOf[Double]
    }
}

object SparkUtil {
    def silenceSpark {
        setLogLevels(Level.WARN, Seq("spark", "org", "akka"))
    }

    def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
        loggers.map {
            loggerName =>
                val logger = Logger.getLogger(loggerName)
                val prevLevel = logger.getLevel()
                logger.setLevel(level)
                loggerName -> prevLevel
        }.toMap
    }
}
