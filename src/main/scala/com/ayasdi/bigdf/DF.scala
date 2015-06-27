/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf

import scala.collection.immutable.Range.Inclusive
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{Column => SColumn}

/**
 * types of joins
 */
object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, Outer = Value
}

/**
 * A DF is a "list of vectors of equal length". It is a 2-dimensional tabular
 * data structure organized as rows and columns.
 *
 * Internally, bigdf's DF is a wrapper on Spark's DataFrame that provides a more
 * pandas-like API e.g. mutability.
 */
case class DF private(var sdf: DataFrame,
                      val options: Options,
                      val name: String) {
  /**
   * number of rows in df, this cannot change. any operation that changes this returns a new df
   * @return number of rows
   */
  lazy val rowCount = sdf.count

  //  lazy val rowIndexRdd = column(0).rdd.zipWithIndex().map(_._2.toDouble)

  /**
   * creates(if not already present) a column with name rowIndexCol that contains a series from zero
   * until rowCount
   * @return column of row indices
   */
  //  lazy val rowIndexCol = {
  //    val col = Column[Double](sc, rowIndexRdd, -1, "rowIndexCol")
  //    setColumn("rowIndexCol", col)
  //    col
  //  }

  /**
   * number of columns in df
   * @return number of columns
   */
  def columnCount = sdf.columns.length

  /**
   * rdd caching storage level. see spark's rdd.cache() for details.
   */
  private val storageLevel: StorageLevel = options.perfTuningOpts.storageLevel

  /**
   * column names in order from first to last numerical index
   * @return array of column names
   */
  def columnNames = sdf.columns

  /**
   * sequence of columns with given indices
   * @param indices sequence of numeric column indices, by default all columns
   * @return sequence of Columns
   */
  def columnsByIndices(indices: Seq[Int] = 0 until columnCount) = {
    indices.map { colIndex => column(colIndex) }
  }

  /**
   * sequence of columns with given names
   * @param colNames sequence of numeric column indices, by default all columns
   * @return sequence of Columns
   */
  def columnsByName(colNames: Seq[String]) = {
    colNames.map { colName => column(colName) }
  }

  /**
   * sequence of columns with given index ranges
   * @param indexRanges sequence of numeric column indices, by default all columns
   * @return sequence of Columns
   */
  def columnsByRanges(indexRanges: Seq[Range]) = for (
    indexRange <- indexRanges;
    index <- indexRange
  ) yield column(index)

  /**
   * schema of this df
   * @return array of tuples. each tuple is a name and type.
   */
  def schema = {
    sdf.schema.map { field => (field.name, SparkUtil.sqlToColType(field.dataType)) }
  }

  /**
   * some metadata for this df
   * @return
   */
  override def toString() = {
    s"Name: $name, #Columns: $columnCount, Options: $options"
  }

  /**
   * convert the DF to an RDD of CSV Strings. columns with compound types like Map, Array are skipped
   * because they cannot be represented in CSV format
   * @param separator use this separator, default is comma
   * @param cols sequence of column names to include in output
   */
  //  def toCSV(separator: String = ",", cols: Seq[String]) = {
  //    val writeColNames = cols.filter(column(_).csvWritable)
  //    val writeCols = writeColNames.map(column(_))
  //
  //    val rows = ColumnZipper.zipAndMap(writeCols) { row => row.mkString(separator) }
  //    val header = writeColNames.mkString(separator)
  //    val headerRdd = sc.parallelize(Array(header))
  //    headerRdd.union(rows)
  //  }
  //
    /**
     * save the DF to a text file
     * @param file save DF in this file
     * @param separator use this separator, default is comma
     * @param singlePart save to a single partition to allow easy transfer to non-HDFS storage
     */
    def writeToCSV(file: String,
                   separator: String = ",",
                   singlePart: Boolean = false,
                   cols: Seq[String] = columnNames): Unit = {
      sdf.write.format("com.databricks.spark.csv").option("header", "true")
    }

  /**
   * save the DF to a parquet file.
   * @param file save DF in this file
   */
  def writeToParquet(file: String,
                     cols: Seq[String] = columnNames): Unit = {
    sdf.select(cols.head, cols.tail: _*).write.parquet(file)
  }

  /**
   * get multiple columns by name, indices or index ranges
   * e.g. myDF("x", "y")
   * or   myDF(0, 1, 6)
   * or   myDF(0 to 0, 4 to 10, 6 to 1000)
   * @param items Sequence of names, indices or ranges. No mix n match yet
   */
  def columns[T: ru.TypeTag](items: Seq[T]): Seq[Column[Any]] = {
    val tpe = ru.typeOf[T]

    require(tpe =:= ru.typeOf[Int] || tpe =:= ru.typeOf[String] ||
      tpe =:= ru.typeOf[Range] || tpe =:= ru.typeOf[Inclusive],
      s"Unexpected argument list of type $tpe")

    if (tpe =:= ru.typeOf[Int])
      columnsByIndices(items.asInstanceOf[Seq[Int]])
    else if (tpe =:= ru.typeOf[String])
      columnsByNames(items.asInstanceOf[Seq[String]])
    else if (tpe =:= ru.typeOf[Range] || tpe =:= ru.typeOf[Inclusive])
      columnsByRanges(items.asInstanceOf[Seq[Range]])
    else null
  }

  /**
   * add/replace a column in this df
   * @param colName name of column, will be overwritten if it exists
   * @param that column to add to this df
   */
  def setColumn(colName: String, that: Column[Any]) = {
    require(that.df.isEmpty && that.index == -1)
    sdf = sdf.withColumn(colName, that.scol) //FIXME: handle if exists case
  }

  /**
   * add/replace a column in this df
   * @param colIndex index of column, will be overwritten if it exists
   * @param that column to add to this df
   */
  //FIXME: unit test
  def setColumn(colIndex: Int, that: Column[Any]) = {
    require(that.df.isEmpty && that.index == -1 && colIndex < columnCount && column(that.name) == null)
    ???
  }

  /**
   * get multiple columns identified by names
   * @param colNames names of columns
   */
  def columnsByNames(colNames: Seq[String]) = colNames.map(column(_))

  /**
   * wrapper on filter to create a new DF from filtered RDD
   * @param cond a predicate to filter on e.g. df("price") > 10
   */
  def where(cond: SColumn): DF = {
    ???
  }

  /**
   * slice the df to get a contiguous subset of rows
   * @param indexRange row index range
   * @return a new df
   */
  //  def rowsByRange(indexRange: Range): DF = {
  //    val pred = (rowIndexCol >= indexRange.start.toDouble) &&
  //      (rowIndexCol <= indexRange.end.toDouble)
  //
  //    where(pred)
  //  }

  /**
   * rename columns
   * @param oldName2New a map of old name to new name
   * @param inPlace true to modify this df, false to create a new one
   */
  def rename(oldName2New: Map[String, String], inPlace: Boolean = true) = {
    var newSdf = sdf
    oldName2New.foreach { case (k, v) =>
      newSdf = newSdf.withColumnRenamed(k, v)
    }

    if (inPlace) {
      sdf = newSdf
    }

    sdf
  }

  /**
   * number of rows that have NA(NaN or empty string)
   * somewhat expensive, don't use this if count of NAs per column suffices
   */
  def countRowsWithNA = {
    //    rowsRddCached = None //fillNA could have mutated columns, recalculate rows
    //    val x = rowsRdd.map { row => if (CountHelper.countNaN(row) > 0) 1 else 0 }
    //    x.reduce {
    //      _ + _
    //    }
  }

  /**
   * number of columns that have NA
   */
  def countColsWithNA = {
    //    columns().map { col => if (col.hasNA) 1 else 0 }.reduce { _ + _ }
  }

  /**
   * drops all row that have NAs
   */
  def dropNA(): Unit = sdf.na.drop()

  /**
   * aggregate one column after grouping by another
   * @param aggByCol the columns to group by
   * @param aggedCol the columns to be aggregated, only one for now TBD
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ru.TypeTag, V: ru.TypeTag, W: ru.TypeTag](aggByCol: String,
                                                             aggedCol: String,
                                                             aggtor: Aggregator[U, V, W]) = {
    //    aggregateWithColumnStrategy(List(aggByCol), aggedCol, aggtor)
  }


  /**
   * aggregate multiple columns after grouping by multiple other columns
   * @param aggByCols sequence of columns to group by
   * @param aggdCols map of columns to be aggregated and their aggregation functions
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ru.TypeTag, V: ru.TypeTag, W: ru.TypeTag](aggByCols: Seq[String],
                                                             aggdCols: Map[String, String]) = {
    val aggdSdf = sdf.groupBy(aggByCols.head, aggByCols.tail:_*).agg(aggdCols)

    new DF(aggdSdf, options, s"aggd:$name")
  }

  /**
   * pivot the df and return a new df
   * e.g. half-yearly sales in <salesperson, period H1 or H2, sales> format
   * Jack, H1, 20
   * Jack, H2, 21
   * Jill, H1, 30
   * Gary, H2, 44
   * becomes "pivoted" to <salesperson, H1 sales, H2 sales>
   * Jack, 20, 21
   * Jill, 30, NaN
   * Gary, NaN, 44
   *
   * The resulting df will typically have fewer rows and more columns
   *
   * @param  keyCol column that has "primary key" for the pivoted df e.g. salesperson
   * @param pivotByCol column that is being removed e.g. period
   * @param pivotedCols columns that are being pivoted e.g. sales, by default all columns are pivoted
   * @return new pivoted DF
   */
  //  def pivot(keyCol: String,
  //            pivotByCol: String,
  //            pivotedCols: List[String] = nameToColumn.keys.toList): DF = {
  //    val grped = groupBy(keyCol)
  //    val pivotValues = column(pivotByCol).colType match {
  //      case ColType.String => column(pivotByCol).distinct.collect.asInstanceOf[Array[String]]
  //      case ColType.Double => column(pivotByCol).distinct.collect.asInstanceOf[Array[Double]]
  //        .map {
  //        _.toString
  //      }
  //      case _ => {
  //        println(s"Pivot does not yet support columns ${column(pivotByCol)}")
  //        null
  //      }
  //    }
  //
  //    val pivotIndex = nameToColumn.getOrElse(pivotByCol, null).index
  //
  //    /*
  //        filter pivot by and key column from output
  //     */
  //    val cleanedPivotedCols = pivotedCols.map {
  //      nameToColumn(_).index
  //    }
  //      .filter { colIndex => colIndex != nameToColumn(pivotByCol).index && colIndex != nameToColumn(keyCol).index }
  //
  //    val newDf = DF(sc, s"${name}_${keyCol}_pivot_${pivotByCol}", options)
  //
  //    /*
  //        add key column to output
  //     */
  //    column(keyCol).colType match {
  //      case ColType.String => newDf.setColumn(s"$keyCol",
  //          Column(sc, grped.map(_._1.asInstanceOf[String])))
  //      case ColType.Double =>  newDf.setColumn(s"$keyCol",
  //          Column(sc, grped.map(_._1.asInstanceOf[Double])))
  //      case _ => {
  //        println(s"Pivot does not yet support columns ${column(keyCol)}")
  //      }
  //    }
  //
  //    /*
  //        add pivoted columns
  //     */
  //    pivotValues.foreach { pivotValue =>
  //      val grpSplit = new PivotHelper(grped, pivotIndex, pivotValue).get
  //
  //      cleanedPivotedCols.foreach { pivotedColIndex =>
  //        nameToColumn(indexToColumnName(pivotedColIndex)).colType match {
  //          case ColType.Double => {
  //            val newColRdd = grpSplit.map {
  //              case (k, v) =>
  //                if (v.isEmpty) Double.NaN else v.head(pivotedColIndex).asInstanceOf[Double]
  //            }
  //            newDf.setColumn(s"D_${indexToColumnName(pivotedColIndex)}@$pivotByCol==$pivotValue",
  //              Column(sc, newColRdd.asInstanceOf[RDD[Double]]))
  //          }
  //          case ColType.String => {
  //            val newColRdd = grpSplit.map {
  //              case (k, v) =>
  //                if (v.isEmpty) "" else v.head(pivotedColIndex).asInstanceOf[String]
  //            }
  //            newDf.setColumn(s"S_${indexToColumnName(pivotedColIndex)}@$pivotByCol==$pivotValue",
  //              Column(sc, newColRdd.asInstanceOf[RDD[String]]))
  //          }
  //          case _ => {
  //            println(s"Pivot does not yet support columns ${column(pivotByCol)}")
  //          }
  //        }
  //      }
  //    }
  //    newDf
  //  }

  /**
   * get a column identified by its name
   * @param colName name of the column
   */
  def column(colName: String) = new Column(sdf.col(colName))

  /**
   * get a column identified by its numerical index
   * @param colIndex index of the column
   */
  def column(colIndex: Int) = new Column(sdf.col(sdf.columns(colIndex)))

  def delete(colName: String): Unit = sdf.drop(colName)

  /**
   * group by using given columns as key
   */
  def groupBy(colName: String) = sdf.groupBy(colName)

  /**
   * print brief description of the DF
   */
  def describe(colNames: String*) = sdf.describe(colNames:_*)

  /**
   * print upto numRows x numCols elements of the dataframe
   */
  def list(numRows: Int = 10, numCols: Int = 10): Unit = sdf.show(numRows)

}

object DF {
  /**
   * create DF from a text file with given separator
   * first line of file is a header
   * For CSV/TSV files only numeric(for now only Double) and String data types are supported
   * @param sc The spark context
   * @param inFile Full path to the input CSV/TSV file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param nParts number of parts to process in parallel
   */
  def apply(sc: SparkContext, inFile: String, separator: Char, nParts: Int, options: Options): DF = {
    if (FileUtils.isDir(inFile)(sc)) fromCSVDir(sc, inFile, ".*", true, separator, nParts, options)
    else fromCSVFile(sc, inFile, separator, nParts, options = options)
  }

  /**
   * create DF from a directory of comma(or other delimiter) separated text files
   * first line of each file is a header
   * Only numeric(for now only Double) and String data types are supported
   * @param sc The spark context
   * @param inDir Full path to the input directory. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param nParts number of parts to process in parallel
   */
  def fromCSVDir(sc: SparkContext,
                 inDir: String,
                 pattern: String,
                 recursive: Boolean,
                 separator: Char,
                 nParts: Int,
                 options: Options): DF = {
    val files = FileUtils.dirToFiles(inDir, recursive, pattern)(sc)
    val numPartitions = if (nParts == 0) 0 else if (nParts >= files.size) nParts / files.size else files.size
    val dfs = files.map { file => fromCSVFile(sc, file, separator, numPartitions) }
    union(sc, dfs)
  }

  def fromColumns(sc: SparkContext, cols: Seq[Column[_]], name: String, options: Options): DF = {
    val sqlContext = new SQLContext(sc)
    var sdf = sqlContext.emptyDataFrame
    cols.foreach { col =>
      sdf = sdf.withColumn(col.name, col.scol)
    }

    new DF(sdf, options, name)
  }

  /**
   * create DF from a directory of comma(or other delimiter) separated text files
   * first line of file is a header
   * Only numeric(for now only Double) and String data types are supported
   * @param sc The spark context
   * @param inFile Full path to the input file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param nParts number of parts to process in parallel
   */
  def fromCSVFile(sc: SparkContext,
                  inFile: String,
                  separator: Char, //FIXME: move to options
                  nParts: Int = 0,
                  schema: Map[String, ColType.EnumVal] = Map(),
                  options: Options = Options()): DF = {
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val sdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(inFile)

    new DF(sdf, options, "fromCSV: $inFile")
  }

  /**
   * create DF from a parquet file. Schema should be flat and contain numeric and string types only(for now)
   * @param sc The spark context
   * @param inFile Full path to the input file. If running on cluster, it should be accessible on all nodes
   */
  def fromParquet(sc: SparkContext,
                  inFile: String,
                  options: Options = Options()): DF = {
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val sdf = sqlContext.read.parquet(inFile)
    require(sdf.schema.fields.forall { field =>
      field.dataType == DoubleType ||
        field.dataType == FloatType ||
        field.dataType == StringType ||
        field.dataType == BinaryType ||
        field.dataType == IntegerType ||
        field.dataType == LongType ||
        field.dataType == ShortType
    }, s"${sdf.schema.fields}")

    new DF(sdf, options, "fromParquet: $inFile")
  }


  /**
   * create a DF given column names and vectors of columns(not rows)
   */
  def apply(sc: SparkContext,
            header: Vector[String],
            vecs: Vector[Vector[Any]],
            dfName: String,
            options: Options): DF = {
    require(header.length == vecs.length, "Shape mismatch")
    require(vecs.map(_.length).toSet.size == 1, "Not a Vector of Vectors")

    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val cols = vecs.map { vec => sc.parallelize(vec) }.toSeq
    val rows = ColumnZipper.zipAndMap(cols) {
      Row.fromSeq(_)
    }

    val colTypes = header.zip(vecs).map { case (colName, col) =>
      col(0) match {
        case c: Double =>
          println(s"Column: ${colName} Type: Double")
          StructField(colName, DoubleType)

        case c: String =>
          println(s"Column: ${colName} Type: String")
          StructField(colName, StringType)
      }
    }

    val sdf = sqlContext.createDataFrame(rows, StructType(colTypes.toArray))
    new DF(sdf, options, dfName)
  }

  /**
   * relational-like join two DFs
   */
  def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = ???

  def compareSchema(a: DF, b: DF) = a.sdf.schema == b.sdf.schema

  def union(sc: SparkContext, dfs: List[DF]) = {
    require(dfs.size > 0)
    require(dfs.tail.forall { df => compareSchema(dfs.head, df) })

    var sdf = dfs.head.sdf
    dfs.tail.foreach { cur => sdf = sdf.unionAll(cur.sdf) }

    new DF(sdf, dfs.head.options, "union")
  }
}
