/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         dataframe on spark
 */

package com.ayasdi.bigdf

import java.nio.file.{Files, Paths}

import scala.collection.mutable

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column => SColumn}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import com.ayasdi.bigdf.Implicits._
import com.databricks.spark.csv.{LineExceptionPolicy, LineParsingOpts}

class DFTest extends FunSuite with BeforeAndAfterAll {
  implicit var sc: SparkContext = _

  override def beforeAll: Unit = {
    SparkUtil.silenceSpark
    System.clearProperty("spark.master.port")
    sc = new SparkContext("local[4]", "DFTest")
  }

  override def afterAll: Unit = {
    sc.stop
  }

  private[bigdf] def makeDF = {
    val h = Vector("a", "b", "c", "Date")
    val v = Vector(Vector(11.0, 12.0, 13.0),
      Vector(21.0, 22.0, 23.0),
      Vector(31.0, 32.0, 33.0),
      Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
    DF(sc, h, v, "makeDF", Options())
  }

  private[bigdf] def makeDFWithNAs = {
    val h = Vector("a", "b", "c", "Date")
    val v = Vector(Vector(11.0, 12.0, null),
      Vector("b1", null, "b3"),
      Vector(31.0, 32.0, 33.0),
      Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
    DF(sc, h, v, "makeDFWithNAs", Options())
  }

  private[bigdf] def makeDFWithNulls = {
    val h = Vector("a", "b", "c", "Date")
    val v = Vector(Vector(-1, 12.0, 13.0),
      Vector("b1", "NULL", "b3"),
      Vector(31.0, 32.0, 33.0),
      Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
    DF(sc, h, v, "makeDFWithNulls", Options())
  }

  private[bigdf] def makeDFWithString = {
    val h = Vector("a", "b", "c", "Date")
    val v = Vector(Vector("11.0", "12.0", "13.0"),
      Vector(21.0, 22.0, 23.0),
      Vector(31.0, 32.0, 33.0),
      Vector(1.36074391383E12, 1.360616948975E12, 1.36055080601E12))
    DF(sc, h, v, "makeDFWithString", Options())
  }

  private[bigdf] def makeDFWithSparseCols = {
    val h = Vector("a", "b", "c", "Sparse")
    val v = Vector(Vector("11.0", "12.0", "13.0"),
      Vector(21.0, 22.0, 23.0),
      Vector(31.0, 32.0, 33.0),
      Vector(mutable.Map("a" -> 1L), mutable.Map("a" -> 2L, "b" -> 0L), mutable.Map("b" -> 1L, "c" -> 10L)))
    DF(sc, h, v, "makeDFWithSparseCols", Options())
  }

  private[bigdf] def makeDFFromCSVFile(file: String, options: Options = Options()) = {
    DF(sc, file, ',', 0, options)
  }

  private[bigdf] def makeDFFromCSVFile2(file: String, options: Options = Options()) = {
    DF(sc, file, ',', 2, options)
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

    assert(df.columnNames === Array("Period", "Customer", "Feature1", "Feature2"))

    assert(df.schema === Array(("Period", ColType.Double),
      ("Customer", ColType.String),
      ("Feature1", ColType.Double),
      ("Feature2", ColType.Double)))

    val df2 = makeDFFromCSVFile2("src/test/resources/pivot.csv")
    assert(df2.columnCount === 4)
    assert(df2.rowCount === 4)
  }

  test("Construct: DF from CSV file with missing fields, fill policy") {
    val df = DF.fromCSVFile(sc, "src/test/resources/missingFields.csv", ',', 0,
      options = Options(lineParsingOpts = LineParsingOpts(badLinePolicy = LineExceptionPolicy.Fill)))
    df.list()
    assert(df.columnCount === 3)
    assert(df.rowCount === 8)
  }

  test("Construct: DF from CSV file with missing fields, ignore policy") {
    val df = DF.fromCSVFile(sc, "src/test/resources/missingFields.csv", ',', 0,
      options = Options(lineParsingOpts = LineParsingOpts(badLinePolicy = LineExceptionPolicy.Ignore)))
    df.list()
    assert(df.columnCount === 3)
    assert(df.rowCount === 2)
  }

  test("Construct: DF from CSV file with missing fields, abort policy") {
    val exception = intercept[SparkException] {
      val df = DF.fromCSVFile(sc, "src/test/resources/missingFields.csv", ',', 0,
        options = Options(lineParsingOpts = LineParsingOpts(badLinePolicy = LineExceptionPolicy.Abort)))
      df.list()
    }
    assert(exception.getMessage.contains("Bad line encountered, aborting"))
  }

  test("Construct: DF from directory of CSV files") {
    val df = makeDFFromCSVFile("src/test/resources/multiFile")
    df.list()
    assert(df.columnCount === 4)
    assert(df.rowCount === 8)

    val df2 = DF.fromCSVDir(sc, "src/test/resources/multiFile", """.*\.csv""", false, ',', 0, Options())
    assert(df2.columnCount === 4)
    assert(df2.rowCount === 8)
  }

  test("Column Index: Refer to a column of a DF") {
    val df = makeDF
    val colA = df("a")
    colA.list()
    assert(colA.colType === ColType.Double)
    assert(colA.index === 0)
    assert(colA.count === 3)
    val col0 = df(0)
    assert(col0 equals colA)
    val colDotA = RichDF(df).a
    assert(colDotA == colA)
  }

  test("Array of column names") {
    val df = makeDF
    assert(df.columnNames === Array("a", "b", "c", "Date"))
  }

  test("Column Index: Refer to non-existent column of a DF") {
    val df = makeDF
    val exception = intercept[org.apache.spark.sql.AnalysisException] {
      df("aa")
    }
    assert(exception.getMessage.contains("Cannot resolve column name"))

    val exception2 = intercept[org.apache.spark.sql.AnalysisException] {
      df("a", "bb")
    }
    assert(exception2.getMessage.contains("Cannot resolve column name"))
  }

  test("Column Index: Refer to multiple columns of a DF") {
    val df = makeDF
    val colSeq = df("a", "b")
    val acol = colSeq(0)
    val bcol = colSeq(1)
    assert(acol.name === "a")
    assert(acol == df("a"))
    assert(bcol.name === "b")
    assert(bcol == df("b"))
  }

  test("Column Index: Slices") {
    val df = makeDF

    val colSeq2 = df(0 to 0, 1 to 3)
    assert(colSeq2.length === 4)
    assert(colSeq2(0) == df("a"))
    assert(colSeq2(1) == df("b"))
    assert(colSeq2(2) == df("c"))
  }

  test("Column Index: Rename") {
    val df = makeDF
    val df2 = df.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"))
    assert((df eq df2) === true)
    assert(df2.columnNames === Array("aa", "bb", "c", "Date"))

    val df3 = makeDF
    val df4 = df3.rename(Map("a" -> "aa", "b" -> "bb", "cc" -> "c"), false)
    assert((df3 ne df4) === true)
    assert(df4.columnNames === Array("aa", "bb", "c", "Date"))
  }

  test("Parsing: Parse mixed doubles") {
    //FIXME: pass options to parser
    //    val options = Options(schemaGuessingOpts = SchemaGuessingOpts(fastSamplingSize = 3))
    //    val df = makeDFFromCSVFile("src/test/resources/mixedDoubles.csv", options)
    //    df.nameToColumn.foreach { col =>
    //      col._2.colType match {
    //        case ColType.Double => col._2.doubleRdd.collect
    //        case _ => null
    //      }
    //    }
    //    assert(df("Feature1").parseErrors.value === 1)
  }

  test("Delete a column") {
    val df = makeDF
    val countBefore = df.columnCount
    val colsBefore = df.columnNames
    df.delete("b")
    val countAfter = df.columnCount
    val colsAfter = df.columnNames

    assert(countAfter === countBefore - 1)
    assert(colsBefore === List("a", "b", "c", "Date"))
    assert(colsAfter === List("a", "c", "Date"))
  }

  test("Parse doubles") {
    //    val df = DF(sc, "src/test/resources/doubles.csv", ',', 0, Options())
    //    assert(df("F1").isDouble)
    //    val parsed = df("F2").doubleRdd.collect()
    //    println(parsed.mkString(", "))
    //    assert(List(0, 2, 3, 4, 5, 6).forall {
    //      parsed(_).isNaN
    //    })
    //    assert(parsed(1) === 2.1)
  }

  test("Schema Dictate") {
    val df = DF.fromCSVFile(sc, "src/test/resources/doubles.csv", ',', 0, Map("F1" -> ColType.String))
    assert(df("F1").isString)
    df.list()
  }

  test("Double to Categorical") {
    //    val df = makeDF
    //    val nCols = df.columnCount
    //    df("cat_a") = df("a").asCategorical
    //    df("cat_a").shortRdd.collect
    //    assert(df("cat_a").parseErrors.value === 0)
    //    assert(df.columnCount === nCols + 1)
  }

  test("Double to Categorical: errors") {
    //    val options = Options(schemaGuessingOpts = SchemaGuessingOpts(fastSamplingSize = 3))
    //    val df = makeDFFromCSVFile("src/test/resources/mixedDoubles.csv", options)
    //    df("cat_f1") = df("Feature1").asCategorical
    //    df("cat_f1").shortRdd.collect
    //    assert(df("cat_f1").parseErrors.value === 2)
  }

  test("Row index") {
    //    val df = makeDF
    //    val df2 = df.rowsByRange(1 until 2)
    //    assert(df2("a").doubleRdd.collect() === Array(12.0, 13.0))
  }

  test("Filter/Select: Double Column comparisons with Scalar") {
    val df = makeDF
    val dfEq12 = df(df("a") === 12)
    assert(dfEq12.rowCount === 1)
    val dfNe12 = df(df("a") !== 12.0)
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
    val dfGt13 = df(df("a") === 133)
    assert(dfGt13.rowCount === 0)
  }

  test("Filter/Select: String Column comparisons with Scalar") {
    val df = makeDFWithString
    val dfEq12 = df(df("a") === "12.0")
    assert(dfEq12.rowCount === 1)
    val dfGt12 = df(df("a") === "12.0")
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
    val dfAeq12AndBeq22 = df(df("a") === 12.0 && df("b") === 22)
    assert(dfAeq12AndBeq22.rowCount === 1)
    val dfAeq12OrBeq23 = df(df("a") === 12 || df("b") === 23)
    assert(dfAeq12OrBeq23.rowCount === 2)
    val dfNotAeq12 = df(!(df("a") === 12))
    assert(dfNotAeq12.rowCount === 2)
    //    val dfAeq12XorBeq23 = df(df("a") === 12 ^^ df("b") === 23)
    //    assert(dfAeq12XorBeq23.rowCount === 2)
  }

  test("NA: Counting NaNs") {
    val df = makeDFWithNAs
    assert(df.countColsWithNA === 2)
    assert(df.countRowsWithNA === 2)
  }

  test("NA: Dropping rows with NaN") {
    val df = makeDFWithNAs
    assert(df.rowCount === 3)
    df.dropNA()
    assert(df.rowCount === 1)
  }

  test("NA: Replacing NA with something else") {
    val df = makeDFWithNAs
    assert(df.countRowsWithNA === 2)
    df.fillNA(Map("a" -> 99.0))
    assert(df.countRowsWithNA === 1)
    df.fillNA(Map("b" -> "hi"))
    assert(df.countRowsWithNA === 0)

    df.list()
  }

  test("NA: Marking a value as NA") {
    //    val df = makeDFWithNulls
    //    assert(df.countRowsWithNA === 0)
    //    df("a").markNA(-1.0)
    //    assert(df.countRowsWithNA === 1)
    //    df("b").markNA("NULL")
    //    assert(df.countRowsWithNA === 2)
  }

  test("Column Ops: New column as custom map of existing one") {
    val df = makeDF

    df("aPlus1") = df("a").map[Double, Double](x => x + 1.0)
    assert(df("aPlus1").doubleRdd.collect === Array(12.0, 13.0, 14.0))

    df("aStr") = df("a").map[Double, String] { x: Double => x.toString }
    assert(df("aStr").stringRdd.collect === Array("11.0", "12.0", "13.0"))
  }


  test("Column Ops: New column as simple function of existing ones") {
    val df = makeDF
    val aa = df("a").doubleRdd.first()
    val bb = df("b").doubleRdd.first()

    df("new") = df("a") + df("b")
    assert(df("new").doubleRdd.first === aa + bb)
    df("new") = df("a") - df("b")
    assert(df("new").doubleRdd.first === aa - bb)
    df("new") = df("a") * df("b")
    assert(df("new").doubleRdd.first === aa * bb)
    df("new") = df("a") / df("b")
    assert(df("new").doubleRdd.first === aa / bb)

    RichDF(df).newCol = RichDF(df).a + RichDF(df).b
    assert(RichDF(df).newCol.doubleRdd.first === aa + bb)
    RichDF(df).newCol = RichDF(df).a.makeCopy
    assert(RichDF(df).newCol.doubleRdd.first === aa)
  }

  test("Column Ops: New column as simple function of existing column and scalar") {
    val df = makeDF
    val aa = df("a").doubleRdd.first

    df("new") = df("a") + 2
    assert(df("new").doubleRdd.first === aa + 2)
    df("new") = df("a") - 2
    assert(df("new").doubleRdd.first === aa - 2)
    df("new") = df("a") * 2
    assert(df("new").doubleRdd.first === aa * 2)
    df("new") = df("a") / 2
    assert(df("new").doubleRdd.first === aa / 2)
  }

  test("Column Ops: New column as custom function of existing ones") {
    val df = makeDF
    df("new") = RichColumnSeq(df("a", "b")).map(TestFunctions2.summer _)
    df.list()
    df("new2") = RichColumnSeq(df("a", "b")).map { (x: Double, y: Double) => x + y }

    assert(df("new").doubleRdd.first === 21 + 11)
    assert(df("new2").doubleRdd.first === 21 + 11)
  }

  test("Aggregate: Catalyst") {
    val df = makeDF
    df("groupByThis") = df("a").map[Double, Double] { x => 1.0 }
    val minOfA = df.aggregate(List("groupByThis"), Min(df("a")))

    df.list()
    minOfA.list()

    assert(minOfA.sdf.first().get(1) === df("a").doubleRdd.collect().min)
  }

  test("Aggregate: Custom") {
    val df = makeDF
    df("groupByThis") = df("a").map[Double, Double] { x => 1.0 }
    val minOfA = df.aggregate(List("groupByThis"), MyMin(df("a")))

    df.list()
    minOfA.list()

    assert(minOfA.sdf.first().get(1) === df("a").doubleRdd.collect().min)
  }

  test("Aggregate: multiple") {
    val df = makeDFFromCSVFile("src/test/resources/aggregate.csv")
    df.list()
    val aggd = df.aggregate(List("Customer", "Month"), Map("Feature1" -> "Sum", "Feature2" -> "Mean"))
    aggd.list()
    assert(aggd.columnCount === 4)
    assert(aggd(aggd("Customer") === "Mohit Jaggi" && aggd("Month") === 2.0)
      .first().get(2) === 9.0)
    assert(aggd(aggd("Customer") === "Jack Jill" && aggd("Month") === 1.0)
      .first().get(3) === 77.0)
  }

  test("Aggregate: SparseSum") {
    val df = makeDFWithSparseCols
    df.list()
    df("groupByThis") = df("a").map[String, String](x => "hey")
    df.list()
    val aggd = df.aggregate(List("groupByThis"), SparseSum(df("Sparse")))
    aggd.list()
    println(aggd.first())
    assert(aggd.first().get(1).asInstanceOf[Map[String, Long]].get("b").get === 1)
    assert(aggd.first().get(1).asInstanceOf[Map[String, Long]].get("a").get === 3)
    assert(aggd.first().get(1).asInstanceOf[Map[String, Long]].get("c").get === 10)
    assert(aggd.first().get(1).asInstanceOf[Map[String, Long]].get("d").isEmpty)
  }

  test("Aggregate: Frequency") {
    val df = makeDFWithString
    df("groupByThis") = df("a").map[String, String](x => "hey")
    df.list()
    val tfs = df.aggregate(List("groupByThis"), Frequency(df("a")))
    println(tfs.first())
  }

  //  test("Pivot") {
  //    val df = makeDFFromCSVFile("src/test/resources/pivot.csv")
  //    df.list()
  //    val df2 = df.pivot("Customer", "Period")
  //    df2.describe()
  //    df2.list()
  //    assert(df2.rowCount === 3)
  //    assert(df2.columnCount === 5)
  //  }

  test("Union") {
    val df1 = makeDF
    val df2 = makeDF
    df1.list()
    val df3 = DF.union(sc, List(df1, df2))
    assert(df3.rowCount === df1.rowCount + df2.rowCount)
    df3.list()
  }

  test("toCSV") {
    val df = makeDF
    val csvRows = df.toCSV(cols = df.columnNames).collect()
    assert(csvRows(0) === "a,b,c,Date")
    assert(csvRows(1) === "11.0,21.0,31.0,1.36074391383E12")
    assert(csvRows(2) === "12.0,22.0,32.0,1.360616948975E12")
    assert(csvRows(3) === "13.0,23.0,33.0,1.36055080601E12")
  }

  test("toParquet") {
    val fileName = "/tmp/x"
    val df = makeDF

    FileUtils.removeAll(fileName)
    df.writeToParquet(fileName)
    assert(true === Files.exists(Paths.get(fileName)))
  }

  test("fromParquet") {
    val fileName = "/tmp/x"
    val df = makeDF
    FileUtils.removeAll(fileName)
    df.writeToParquet(fileName)
    assert(true === Files.exists(Paths.get(fileName)))

    val df2 = DF.fromParquet(sc, fileName)

    println(df.columnNames.mkString)
    println(df2.columnNames.mkString)

    assert(df2.columnNames === df.columnNames)
    assert(df.sdf.intersect(df2.sdf).count === df.sdf.count)
  }

}

class DFTestWithKryo extends DFTest {
  override def beforeAll {
    SparkUtil.silenceSpark()
    System.clearProperty("spark.master.port")

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("DFTestWithKryo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc = new SparkContext(conf)
  }
}

case class MyMin(child: Expression) extends PartialAggregate with trees.UnaryNode[Expression] {
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def toString: String = s"MYMIN($child)"

  override def asPartial: SplitEvaluation = {
    val partialMin = Alias(MyMin(child), "MyPartialMin")()
    SplitEvaluation(MyMin(partialMin.toAttribute), partialMin :: Nil)
  }

  override def newInstance(): MyMinFunction = new MyMinFunction(child, this)
}

case class MyMinFunction(expr: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null) // Required for serialization.

  val currentMin: MutableLiteral = MutableLiteral(null, expr.dataType)
  val cmp = GreaterThan(currentMin, expr)

  override def update(input: Row): Unit = {
    if (currentMin.value == null) {
      currentMin.value = expr.eval(input)
    } else if (cmp.eval(input) == true) {
      currentMin.value = expr.eval(input)
    }
  }

  override def eval(input: Row): Any = currentMin.value
}


case object TestFunctions {
  def summer(cols: Array[Any]) = {
    (cols(0), cols(1)) match {
      case (a: Double, b: Double) => a + b
    }
  }
}

case object TestFunctions2 {
  def summer(a1: Double, a2: Double) = a1 + a2
}

