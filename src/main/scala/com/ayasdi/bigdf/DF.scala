/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER

import scala.collection.mutable.HashMap
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}
import scala.language.dynamics

/**
 * types of joins
 */
object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, Outer = Value
}

/**
 * Data Frame is a map of column name to an RDD containing that column.
 * Constructor is private, instances are created by factory calls(apply) in
 * companion object.
 * Number of rows cannot change. Columns can be added, removed, mutated.
 * The following types of columns are supported:
 * Double
 * Float
 * String
 * Category: stored as a Short category id
 */
case class DF private(val sc: SparkContext,
                      val cols: HashMap[String, Column[Any]] = new HashMap[String, Column[Any]],
                      val colIndexToName: HashMap[Int, String] = new HashMap[Int, String],
                      val name: String) extends Dynamic {
  /**
   * number of rows in df
   * @return number of rows
   */
  lazy val rowCount = {
    require(columnCount > 0)
    cols.head._2.rdd.count
  }

  /**
   * number of columns in df
   * @return number of columns
   */
  def columnCount = cols.size

  /**
   * for large number of columns, column based filtering is faster. it is the default.
   * try changing this to true for DF with few columns
   */
  var filterWithRowStrategy = false

  /**
   * default rdd caching storage level
   */
  var defaultStorageLevel: StorageLevel = MEMORY_ONLY_SER

  private var rowsRddCached: Option[RDD[Array[Any]]] = None
  private def rowsRdd = {
    if(rowsRddCached.isEmpty) {
      rowsRddCached = Some(computeRows)
      rowsRddCached.get.setName(s"${name}").persist(defaultStorageLevel)
    }
    rowsRddCached.get
  }

  def columnNames = {
    (0 until columnCount).map { colIndex => colIndexToName(colIndex) }.toArray
  }

  def columns(indices: Seq[Int] = 0 until columnCount) = {
    indices.map { colIndex => cols(colIndexToName(colIndex)) }
  }

  override def toString() = {
    "Silence is golden" //otherwise prints too much stuff
  }

  /**
   * convert the DF to an RDD of CSV Strings
   * @param separator use this separator, default is comma
   */
  private[bigdf] def toCSV(separator: String = ",") = {
    val rows = ColumnZipper.zipAndMap(columns()) { row => row.mkString(separator) }
    val header = (0 until columnCount).map { colIndex => colIndexToName(colIndex) }
                                      .mkString(separator)
    val headerRdd = sc.parallelize(Array(header))
    headerRdd.union(rows)
  }

  /**
   * save the DF to a text file
   * @param file save DF in this file
   * @param separator use this separator, default is comma
   */
  def writeToCSV(file: String, separator: String = ","): Unit = {
    toCSV(separator).saveAsTextFile(file)
  }

  /**
   * save the DF to a parquet file
   * @param file save DF in this file
   */
  def writeToParquet(file: String): Unit = {
    require(columns().forall(col => col.isDouble || col.isString
                            || col.isFloat || col.isShort)) //TODO: support other types

    val fields = columnNames.map { colName => StructField(colName, cols(colName).sqlType, true) }
    val rowRdd = ColumnZipper.zipAndMap(columns()) { Row.fromSeq(_) }
    val rowsSchemaRdd = new SQLContext(sc).applySchema(rowRdd, StructType(fields))
    rowsSchemaRdd.saveAsParquetFile(file)
  }

  /**
   * get a column identified by name
   * @param colName name of the column
   */
  def apply(colName: String) = column(colName)

  /**
   * get a column with given index
   * @param index column index
   * @return
   */
  def apply(index: Int): Column[Any] = cols(colIndexToName(index))

  /**
   * get multiple columns by name, indices or index ranges
   * e.g. myDF("x", "y")
   * or   myDF(0, 1, 6)
   * or   myDF(0 to 0, 4 to 10, 6 to 1000)
   * @param items Sequence of names, indices or ranges. No mix n match yet
   */
  def apply[T: ru.TypeTag](items: T*): ColumnSeq = {
    val tpe = ru.typeOf[T]

    require(tpe =:= ru.typeOf[Int] || tpe =:= ru.typeOf[String] ||
            tpe =:= ru.typeOf[Range] || tpe =:= ru.typeOf[Range.Inclusive],
            s"Unexpected argument list of type $tpe")

    if (tpe =:= ru.typeOf[Int])
      columnsByIndices(items.asInstanceOf[Seq[Int]])
    else if (tpe =:= ru.typeOf[String])
      columnsByNames(items.asInstanceOf[Seq[String]])
    else if (tpe =:= ru.typeOf[Range] || tpe =:= ru.typeOf[Range.Inclusive])
      columnsByRanges(items.asInstanceOf[Seq[Range]])
    else null
  }

  /**
   * refer to a column 'c' in DF 'df' as df.c equivalent to df("c")
   */
  def selectDynamic(colName: String) = column(colName)

  /**
   * get multiple columns identified by names
   * @param colNames names of columns
   */
  def columnsByNames(colNames: Seq[String]) = {
    val selectedCols = for (colName <- colNames)
      yield (colName, cols.getOrElse(colName, null))
    if (selectedCols.exists(null == _._2)) {
      val notFound = selectedCols.filter(null == _._2)
      println("You sure? I don't know about these columns" + notFound.mkString(","))
      null
    } else {
      new ColumnSeq(selectedCols)
    }
  }

  /**
   * get columns by ranges of numeric indices. invalid indices are silently ignored.
   * FIXME: solo has to be "5 to 5" for now, should be just "5"
   * @param indexRanges sequence of ranges like List(1 to 5, 13 to 15)
   */
  def columnsByRanges(indexRanges: Seq[Range]) = {
    val selectedCols = for (
      indexRange <- indexRanges;
      index <- indexRange;
      if (colIndexToName(index) != null)
    ) yield (colIndexToName(index), cols(colIndexToName(index)))

    new ColumnSeq(selectedCols)
  }

  /**
   * get columns by sequence of numeric indices
   * @param indices Sequence of indices like List(1, 3, 13)
   */
  def columnsByIndices(indices: Seq[Int]) = {
    require(indices.forall {
      colIndexToName.contains(_)
    })

    val indexRanges = indices.map { i => i to i}
    columnsByRanges(indexRanges)
  }

  /**
   * allow omitting keyword "where"
   */
  def apply(cond: Predicate) = where(cond)

  /**
   * wrapper on filter to create a new DF from filtered RDD
   * @param cond a predicate to filter on e.g. df("price") > 10
   */
  def where(cond: Predicate): DF = {
    if (filterWithRowStrategy) {
      fromRows(filterRowStrategy(cond))
    } else {
      DF(this, filterColumnStrategy(cond))
    }
  }

  /*
   * usually more efficient if there are a few columns
   */
  private def filterRowStrategy(cond: Predicate) = {
    rowsRdd.filter(row => cond.checkWithRowStrategy(row))
  }

  /*
   * more efficient if there are a lot of columns
   */
  private def filterColumnStrategy(cond: Predicate) = {
    println (cond)
    val zippedColRdd = ColumnZipper.makeRows(this, cond.colSeq)
    val colMap = new HashMap[Int, Int] //FIXME: switch to a fast map here
    var i = 0
    cond.colSeq.foreach { colIndex =>
      colMap(colIndex) = i
      i += 1
    }
    zippedColRdd.map(cols => cond.checkWithColStrategy(cols, colMap))
  }

  private def fromRows(rows: RDD[Array[Any]]) = {
    val newDf = new DF(sc, colIndexToName = this.colIndexToName.clone, name = this.name + "-fromRows")
    //replace col.rdd in this df to get newDf
    this.cols.foreach { case(colName, col) =>
      val i = col.index
      col.colType match {

        case ColType.Double =>  {
          val colRdd = rows.map { row => row(i).asInstanceOf[Double] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.Float =>   {
          val colRdd = rows.map { row => row(i).asInstanceOf[Float] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.Short =>  {
          val colRdd = rows.map { row => row(i).asInstanceOf[Short] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.String => {
          val colRdd = rows.map { row => row(i).asInstanceOf[String] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.ArrayOfString => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Array[String]] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.ArrayOfDouble => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Array[Double]] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.MapOfStringToFloat => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Map[String, Float]] }
          newDf.cols(colName) = Column(sc, colRdd, i)
        }

        case ColType.Undefined =>
          println(s"fromRows: Unhandled type ${col.tpe}")
      }

    }
    newDf
  }

  /**
   * rename columns, modify same DF or make a new one
   */
  def rename(columns: Map[String, String], inPlace: Boolean = true) = {
    val df = if (inPlace == false) new DF(sc, cols.clone, colIndexToName.clone, name) else this

    columns.foreach {
      case (oldName, newName) =>
        val col = df.cols.remove(oldName)
        if (!col.isEmpty) {
          val (i, n) = df.colIndexToName.find(x => x._2 == oldName).get
          df.colIndexToName.put(i, newName)
          df.cols.put(newName, col.get)
          println(s"${oldName}[${i}] --> ${newName}")
        } else {
          println(s"Wazz that? I can't find ${oldName}, skipping it")
        }
    }
    df
  }

  /**
   * number of rows that have NA(NaN or empty string)
   * somewhat expensive, don't use this if count of NAs per column suffices
   */
  def countRowsWithNA = {
    rowsRddCached = None //fillNA could have mutated columns, recalculate rows
    val x = rowsRdd.map { row => if (CountHelper.countNaN(row) > 0) 1 else 0}
    x.reduce {
      _ + _
    }
  }

  /**
   * number of columns that have NA
   */
  def countColsWithNA = {
    cols.map { col => if (col._2.hasNA) 1 else 0}.reduce {
      _ + _
    }
  }

  /**
   * create a new DF after removing all rows that had NAs
   */
  def dropNA(rowStrategy: Boolean = filterWithRowStrategy) = {
    if(rowStrategy) {
      dropNAWithRowStrategy
    } else {
      dropNAWithColumnStrategy
    }
  }
  private def dropNAWithRowStrategy = {
    val rows = rowsRdd.filter { row => CountHelper.countNaN(row) == 0 }
    fromRows(rows)
  }
  private def dropNAWithColumnStrategy: DF = {
    ???
  }

  /**
   * group by multiple columns, uses a lot of memory. try to use aggregate(By) instead if possible
   */
  def groupBy(colNames: String*) = {
    keyBy(colNames, rowsRdd).groupByKey
  }

  /*
   * columns are zip'd together to get rows, expensive operation
   */
  private def computeRows: RDD[Array[Any]] = {
    ColumnZipper.makeRows(this, (0 until columnCount).toList)
  }

  /*
   * augment with key, resulting RDD is a pair with k=array[any]
   * and value=array[any]
   */
  private def keyBy(colNames: Seq[String], valueRdd: RDD[Array[Any]]) = {
    val columns = colNames.map {
      cols(_).index
    }
    ColumnZipper.makeRows(this, columns)
  }.zip(valueRdd)

  /**
   * aggregate one column after grouping by another
   * @param aggByCol the columns to group by
   * @param aggedCol the columns to be aggregated, only one for now TBD
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggByCol: String, aggedCol: String, aggtor: Aggregator[U, V, W]) = {
    aggregateWithColumnStrategy(List(aggByCol), aggedCol, aggtor)
  }

  /**
   * aggregate one column after grouping by another
   */
  private def aggregateWithColumnStrategy[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggdByCols: Seq[String], aggdCol: String, aggtor: Aggregator[U, V, W]) = {

    val wtpe = classTag[W]
    if (aggtor != AggCount) require(cols(aggdCol).compareType(classTag[U]))

    val newDf = DF(sc, s"${name}/${aggdCol}_aggby_${aggdByCols.mkString(";")}")

    val aggedRdd = keyBy(aggdByCols, aggdCol).asInstanceOf[RDD[(List[Any], U)]]
      .combineByKey(aggtor.convert, aggtor.mergeValue, aggtor.mergeCombiners)

    // columns of key
    var i = 0
    aggdByCols.foreach { aggdByCol =>
      val j = i
      cols(aggdByCol).colType match {
        case ColType.Double => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Double]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.Float => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Float]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.Short => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Short]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.String => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[String]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.ArrayOfString => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Array[String]]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.ArrayOfDouble => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Array[Double]]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.MapOfStringToFloat => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Map[String, Float]]
          }
          newDf.update(aggdByCol, Column(sc, col1, newDf.columnCount))
        }
        case ColType.Undefined => {
          println(s"ERROR: Undefined column type while aggregating ${aggdByCol}")
        }
      }

      i += 1
    }

    // finalize the aggregations and add column of that
    if (wtpe == classTag[Double]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }.asInstanceOf[RDD[Double]]
      newDf.update(aggdCol, Column(sc, col1, 1))
    } else if (wtpe == classTag[String]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }.asInstanceOf[RDD[String]]
      newDf.update(aggdCol, Column(sc, col1, newDf.columnCount))
    } else {
      println("ERROR: aggregate value type" + wtpe)
    }

    newDf
  }

  private def keyBy(colNames: Seq[String], valueCol: String) = {
    val columns = colNames.map {
      cols(_).index
    }
    ColumnZipper.makeList(this, columns)
  }.zip(cols(valueCol).rdd)

  /**
   * aggregate multiple columns after grouping by multiple other columns
   * @param aggByCols sequence of columns to group by
   * @param aggedCol sequence of columns to be aggregated
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ClassTag, V: ClassTag, W: ClassTag]
  (aggByCols: Seq[String], aggedCol: Seq[String], aggtor: Aggregator[U, V, W]) = {
    aggregateWithColumnStrategy(aggByCols, aggedCol.head, aggtor)
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
  def pivot(keyCol: String, pivotByCol: String,
            pivotedCols: List[String] = cols.keys.toList): DF = {
    val grped = groupBy(keyCol)
    val pivotValues =  column(pivotByCol).colType match {
      case ColType.String =>  column(pivotByCol).distinct.collect.asInstanceOf[Array[String]]
      case ColType.Double =>  column(pivotByCol).distinct.collect.asInstanceOf[Array[Double]]
                              .map { _.toString }
      case _ => {
        println(s"Pivot does not yet support columns ${column(pivotByCol)}")
        null
      }
    }

    val pivotIndex = cols.getOrElse(pivotByCol, null).index
    val cleanedPivotedCols = pivotedCols.map { cols(_).index}.filter { _ != cols(pivotByCol).index }

    val newDf = DF(sc, s"${name}_${keyCol}_pivot_${pivotByCol}")
    pivotValues.foreach { pivotValue =>
      val grpSplit = new PivotHelper(grped, pivotIndex, pivotValue).get

      cleanedPivotedCols.foreach { pivotedColIndex =>
        cols(colIndexToName(pivotedColIndex)).colType match {
          case ColType.Double => {
            val newColRdd = grpSplit.map {
              case (k, v) =>
                if (v.isEmpty) Double.NaN else v.head(pivotedColIndex).asInstanceOf[Double]
            }
            newDf.update(s"D_${colIndexToName(pivotedColIndex)}@$pivotByCol==$pivotValue",
              Column(sc, newColRdd.asInstanceOf[RDD[Double]]))
          }
          case ColType.String =>  {
            val newColRdd = grpSplit.map {
              case (k, v) =>
                if (v.isEmpty) "" else v.head(pivotedColIndex).asInstanceOf[String]
            }
            newDf.update(s"S_${colIndexToName(pivotedColIndex)}@$pivotByCol==$pivotValue",
              Column(sc, newColRdd.asInstanceOf[RDD[String]]))
          }
          case _ => {
            println(s"Pivot does not yet support columns ${column(pivotByCol)}")
          }
        }
      }
    }
    newDf
  }

  /**
   * get a column identified by name
   * @param colName name of the column
   */
  def column(colName: String) = {
    val col = cols.getOrElse(colName, null)
    if (null == col) println(s"${colName} not found")
    col
  }

  /**
   * update a column, add or replace
   */
  def update(colName: String, that: Column[Any]) = {
    val col = cols.getOrElse(colName, null)

    cols.put(colName, that)
    if (null != col) {
      println(s"Replaced Column: ${colName}")
      that.index = col.index
    } else {
      println(s"New Column: ${colName}")
      val colIndex = colIndexToName.size
      colIndexToName.put(colIndex, colName)
      that.index = colIndex
    }
    rowsRddCached = None //invalidate cached rows
    //FIXME: incremental computation of rows
  }

  /**
   * update a column "c" of DF "df" like df.c = .. equivalent df("c") = ...
   */
  def updateDynamic(colName: String)(that: Column[Any]) = update(colName, that)

  /**
   * group by a column, uses a lot of memory. try to use aggregate(By) instead if possible
   */
  def groupBy(colName: String) = {
    keyBy(colName, rowsRdd).groupByKey
  }

  private def keyBy(colName: String, valueRdd: RDD[Array[Any]]) = {
    cols(colName).rdd
  }.zip(valueRdd)

  /**
   * print brief description of the DF
   */
  def describe(): Unit = {
    cols.foreach {
      case (name, col) =>
        println(s"${name}:")
        println(col.toString)
    }
  }

  /**
   * print upto numRows x numCols elements of the dataframe
   */
  def list(numRows: Int = 10, numCols: Int = 10): Unit = {
    println(s"Dimensions: $rowCount x $columnCount")
    val nRows = Math.min(numRows, rowCount)
    val nCols = Math.min(numCols, columnCount)
    val someCols = (0 until nCols).toList.map { colIndex => cols(colIndexToName(colIndex)) }
    val someRows = someCols.map { col => col.head(nRows.toInt).toList }.transpose
    someRows.foreach { row =>
      println(row.mkString("| ","\t", " |"))
    }
  }

  /*
   * add column keys, returns number of columns added
   */
  private def addHeader(header: Array[String]): Int = {
    addHeader(header.iterator)
  }

  /*
   * add column keys, returns number of columns added
   */
  private def addHeader(header: Iterator[String]) = {
    var i = 0
    header.foreach { colName =>
      val cleanedColName = if (cols.contains(colName)) {
        println(s"**WARN**: Duplicate column ${colName} renamed")
        colName + "_"
      } else {
        colName
      }
      cols.put(cleanedColName, null)
      colIndexToName.put(i, cleanedColName)
      i += 1
    }
    i
  }

}

object DF {
  /**
   * create DF from a text file with given separator
   * first line of file is a header
   * For CSV/TSV files only numeric(for now only Double) and String data types are supported
   * @param inFile Full path to the input CSV/TSV file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   */
  def apply(inFile: String, separator: Char, fasterGuess: Boolean)(implicit sc: SparkContext): DF = {
    apply(sc, inFile, separator, fasterGuess)
  }

  /**
   * create DF from a text file with given separator
   * first line of file is a header
   * For CSV/TSV files only numeric(for now only Double) and String data types are supported
   * @param sc The spark context
   * @param inFile Full path to the input CSV/TSV file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param fasterGuess Just use true unless you are having trouble
   */
  def apply(sc: SparkContext, inFile: String, separator: Char, fasterGuess: Boolean): DF = {
    if(FileUtils.isDir(inFile)(sc)) fromDir(sc, inFile, separator, fasterGuess)
    else fromFile(sc, inFile, separator, fasterGuess)
  }

  def fromDir(sc: SparkContext, inDir: String, separator: Char, fasterGuess: Boolean): DF = {
    val dfs = FileUtils.dirToFiles(inDir)(sc).map { file => fromFile(sc, file, separator, fasterGuess) }
    union(sc, dfs)
  }

  /**
   * create DF from a text file with given separator
   * first line of file is a header
   * For CSV/TSV files only numeric(for now only Double) and String data types are supported
   * @param sc The spark context
   * @param inFile Full path to the input CSV/TSV file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param fasterGuess Just use true unless you are having trouble
   */
  def fromFile(sc: SparkContext, inFile: String, separator: Char, fasterGuess: Boolean): DF  = {
    val df: DF = DF(sc, inFile)
    df.defaultStorageLevel = MEMORY_ONLY_SER //FIXME: allow changing this
    val file = sc.textFile(inFile)

    // parse header line
    val firstLine = file.first
    val header = new LineCsvParser(separator).parseLine(firstLine)
    println(s"Found ${header.size} columns in header")
    df.addHeader(header)

    // parse data lines
    val dataLines = file.mapPartitionsWithIndex({
      case (partitionIndex, iter) => if (partitionIndex == 0) iter.drop(1) else iter
    }, true)
    dataLines.setName(s"data/$inFile")

    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        new BulkCsvParser(iter, split, separator)
      }
    }, true).persist(df.defaultStorageLevel)

    // FIXME: see if this is a lot faster with an rdd.unzip function
    val columns = for (i <- 0 until df.columnCount) yield {
      rows.map { row => row(i) }
        .persist(df.defaultStorageLevel)
    }

    var i = 0
    val firstFewRows = if(fasterGuess) rows.take(5) else null
    columns.foreach { col =>
      val t = if (fasterGuess) {
        SchemaUtils.guessTypeByFirstFew(firstFewRows.map { row => row(i) })
      } else {
        SchemaUtils.guessType(sc, columns(i))
      }
      col.setName(s"$t/$inFile/${df.colIndexToName(i)}")
      println(s"Column: ${df.colIndexToName(i)} \t\t\tGuessed Type: ${t}")
      if (t == ru.typeOf[Double]) {
        df.cols.put(df.colIndexToName(i), Column.asDoubles(sc, col, i, df.defaultStorageLevel))
        col.unpersist()
      } else {
        df.cols.put(df.colIndexToName(i), Column(sc, col, i))
        col.persist(df.defaultStorageLevel)
      }
      i += 1
    }
    //rows.unpersist()
    df
  }

  /**
   * create a DF given column names and vectors of columns(not rows)
   */
  def apply(sc: SparkContext, header: Vector[String], vec: Vector[Vector[Any]]): DF = {
    require(header.length == vec.length)
    require(vec.map {
      _.length
    }.toSet.size == 1)

    val df = DF(sc, "from_vector")
    df.addHeader(header.toArray)

    var i = 0
    vec.foreach { col =>
      col(0) match {
        case c: Double =>
          println(s"Column: ${df.colIndexToName(i)} Type: Double")
          df.cols.put(df.colIndexToName(i),
            Column(sc, sc.parallelize(col.asInstanceOf[Vector[Double]]), i))

        case c: String =>
          println(s"Column: ${df.colIndexToName(i)} Type: String")
          df.cols.put(df.colIndexToName(i),
            Column(sc, sc.parallelize(col.asInstanceOf[Vector[String]]), i))
      }
      i += 1
    }
    df
  }

  /**
   * create a DF from a ColumnSeq
   */
  def apply(sc: SparkContext, colSeq: ColumnSeq): DF = {
    val df = DF(sc, "colseq")
    val header = colSeq.cols.map {
      _._1
    }
    val columns = colSeq.cols.map {
      _._2
    }

    df.addHeader(header.toArray)
    var i = 0
    columns.foreach { col =>
      println(s"Column: ${df.colIndexToName(i)} Type: Double")
      df.cols.put(df.colIndexToName(i), col)
      i += 1
    }
    df
  }

  /**
   * create a DF from a Column
   */
  def apply(sc: SparkContext, name: String, col: Column[Double]): DF = {
    val df = DF(sc, "col")
    val i = df.addHeader(Array(name))
    df.cols.put(df.colIndexToName(i - 1), col)
    df
  }

  /**
   * make a filtered DF
   */
  def apply(df: DF, filtration: RDD[Boolean]) = {
    val cols = df.cols.clone
    for (i <- 0 until df.columnCount) {
      def applyFilter[T: ClassTag](in: RDD[T]) = {
        ColumnZipper.filterBy(in, filtration)
      }
      val colName = df.colIndexToName(i)
      val col = cols(colName)

      col.colType match {
        case ColType.Double =>  cols(colName) = Column(df.sc, applyFilter(col.doubleRdd), i)
        case ColType.Float =>  cols(colName) = Column(df.sc, applyFilter(col.floatRdd), i)
        case ColType.Short =>  cols(colName) = Column(df.sc, applyFilter(col.shortRdd), i)
        case ColType.String =>  cols(colName) = Column(df.sc, applyFilter(col.stringRdd), i)
        case ColType.ArrayOfString => cols(colName) = Column(df.sc, applyFilter(col.arrayOfStringRdd), i)
        case ColType.ArrayOfDouble => cols(colName) = Column(df.sc, applyFilter(col.arrayOfDoubleRdd), i)
        case ColType.MapOfStringToFloat => cols(colName) = Column(df.sc, applyFilter(col.mapOfStringToFloatRdd), i)
        case ColType.Undefined =>  println(s"ERROR: Column type UNKNOWN for ${colName}")
      }
    }
    println(cols)
    new DF(df.sc, cols, df.colIndexToName.clone, s"filtered_${df.name}")
  }

  /**
   * relational-like join two DFs
   */
  def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val newDf = DF(sc, s"${left.name}-join-${right.name}")
    val joinedRows = joinRdd(sc, left, right, on, how)
    val firstRow = joinedRows.first

    def leftValue(row: (Any, (Array[Any], Array[Any]))) = row._2._1
    def rightValue(row: (Any, (Array[Any], Array[Any]))) = row._2._2

    /*
     * if the two DFs being joined have columns with same names, prefix them with
     * left_ and right_ in the joined DF
     */
    def joinedColumnName(name: String, start: Int) = {
      val collision = !left.cols.keys.toSet.intersect(right.cols.keys.toSet).isEmpty
      if (!collision) {
        name
      } else {
        if (start == 0) {
          "left_" + name
        } else {
          "right_" + name
        }
      }
    }

    /*
     * add left or right columns to the DF
     * start = 0 for left, start = left.numCols for right
     */
    def addCols(curDf: DF, start: Int, partGetter: ((Any, (Array[Any], Array[Any]))) => Array[Any]) = {
      for (joinedIndex <- start until start + curDf.columnCount) {
        val origIndex = joinedIndex - start
        val newColName = joinedColumnName(curDf.colIndexToName(origIndex), start)

        val column = curDf(origIndex).colType match {
          case ColType.Double => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Double] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.Float => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Float] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.String => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[String] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.ArrayOfString => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Array[String]]}
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.ArrayOfDouble => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Array[Double]]}
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.Short => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Short]}
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.MapOfStringToFloat => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Map[String, Float]]}
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.Undefined => {
            println(s"ERROR: Column type UNKNOWN for ${newColName}")
            assert(false)
            null
          }
        }

        newDf.cols.put(newColName, column)
        newDf.colIndexToName(joinedIndex) = newColName
      }
    }

    addCols(left, 0, leftValue)
    addCols(right, left.columnCount, rightValue)

    newDf
  }

  /**
   * make an empty DF
   */
  def apply(sc: SparkContext, name: String) = {
    new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String], name)
  }

  def joinRdd(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val leftWithKey = left.cols(on).rdd.zip(left.rowsRdd)
    val rightWithKey = right.cols(on).rdd.zip(right.rowsRdd)
    leftWithKey.join(rightWithKey)
  }

  def union(sc: SparkContext, dfs: List[DF]) = {
    require(dfs.size > 0)

    val cols = dfs.head.cols.clone
    val df = dfs.head

    def unionRdd[T: ClassTag](rdds: List[RDD[T]]) = new UnionRDD[T](sc, rdds)

    for(i <- 0 until dfs.head.columnCount) {
      val unionCol = dfs.head.columns()(i).colType match {
        case ColType.Double => {
          val cols = dfs.map { df => df(i).doubleRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Float => {
          val cols = dfs.map { df => df(i).floatRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.String => {
          val cols = dfs.map { df => df(i).stringRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.ArrayOfString => {
          val cols = dfs.map { df => df(i).arrayOfStringRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.ArrayOfDouble => {
          val cols = dfs.map { df => df(i).arrayOfDoubleRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Short => {
          val cols = dfs.map { df => df(i).shortRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.MapOfStringToFloat => {
          val cols = dfs.map { df => df(i).mapOfStringToFloatRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Undefined => {
          println(s"ERROR: Column type UNKNOWN for ${df(i)}")
          assert(false)
          null
        }
      }
      val colName = df.colIndexToName(i)
      cols(colName) = unionCol
    }

    new DF(df.sc, cols, df.colIndexToName.clone, s"filtered_${df.name}")
  }
}
