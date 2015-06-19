/* Ayasdi Inc. Copyright 2014 - all rights reserved. */
/**
 * @author mohit
 *         big dataframe on spark
 */
package com.ayasdi.bigdf

import scala.collection.immutable.Range.Inclusive
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.language.dynamics
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import com.ayasdi.bigdf.Implicits._
import com.ayasdi.bigdf.readers.{BulkCsvReader, LineCsvReader}

/**
 * types of joins
 */
object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, Outer = Value
}

/**
 * A DataFrame is a "list of vectors of equal length". It is a 2-dimensional tabular
 * data structure organized as rows and columns.
 *
 * Internally, bigdf's DF is a map of column names to RDD(s) containing those columns.
 * Constructor is private, instances are created by factory calls(apply) in the
 * companion object.
 * Number of rows cannot change. Columns can be added, removed, modified.
 * The following types of columns are supported:
 * Double
 * Float
 * String
 * Category: stored as a Short category id
 * Map: A column "family" or sparse columns i.e. most rows have "default" value for most columns
 * Array: Generally an intermediate step in a transform that prepares several items for the next step
 */
case class DF private(val sc: SparkContext,
                      val nameToColumn: HashMap[String, Column[Any]] = new HashMap[String, Column[Any]],
                      val indexToColumnName: HashMap[Int, String] = new HashMap[Int, String],
                      val options: Options,
                      val name: String) {
  /**
   * number of rows in df, this cannot change. any operation that changes this returns a new df
   * @return number of rows
   */
  lazy val rowCount = {
    require(columnCount > 0, "No columns found")
    column(0).count
  }

  lazy val rowIndexRdd = column(0).rdd.zipWithIndex().map(_._2.toDouble)

  /**
   * creates(if not already present) a column with name rowIndexCol that contains a series from zero
   * until rowCount
   * @return column of row indices
   */
  lazy val rowIndexCol = {
    val col = Column[Double](sc, rowIndexRdd, -1, "rowIndexCol")
    setColumn("rowIndexCol", col)
    col
  }

  /**
   * number of columns in df
   * @return number of columns
   */
  def columnCount = nameToColumn.size

  /**
   * for large number of columns, column based filtering is faster. it is the default.
   * try changing this to true for DF with few columns
   */
  private val filterWithRowStrategy = options.perfTuningOpts.filterWithRowStrategy

  /**
   * rdd caching storage level. see spark's rdd.cache() for details.
   */
  private val storageLevel: StorageLevel = options.perfTuningOpts.storageLevel

  private var rowsRddCached: Option[RDD[Array[Any]]] = None

  private def rowsRdd = {
    if (rowsRddCached.isEmpty) {
      rowsRddCached = Some(computeRows)
      rowsRddCached.get.setName(s"rows_for_${name}").persist(storageLevel)
    }
    rowsRddCached.get
  }

  /**
   * column names in order from first to last numerical index
   * @return array of column names
   */
  def columnNames = {
    (0 until columnCount).map { colIndex => indexToColumnName(colIndex) }.toArray
  }

  /**
   * sequence of columns with given indices
   * @param indices sequence of numeric column indices, by default all columns
   * @return sequence of Columns
   */
  def columns(indices: Seq[Int] = 0 until columnCount) = {
    indices.map { colIndex => column(indexToColumnName(colIndex)) }
  }

  /**
   * schema of this df
   * @return array of tuples. each tuple is a name and type.
   */
  def schema = {
    columnNames.map { name => (name, column(name).colType) }
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
  def toCSV(separator: String = ",", cols: Seq[String]) = {
    val writeColNames = cols.filter(column(_).csvWritable)
    val writeCols = writeColNames.map(column(_))

    val rows = ColumnZipper.zipAndMap(writeCols) { row => row.mkString(separator) }
    val header = writeColNames.mkString(separator)
    val headerRdd = sc.parallelize(Array(header))
    headerRdd.union(rows)
  }

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
    if (singlePart) {
      toCSV(separator, cols).coalesce(1).saveAsTextFile(file)
    } else {
      toCSV(separator, cols).saveAsTextFile(file)
    }
  }

  /**
   * save the DF to a parquet file. skips compound columns like map and array, these will be supported later.
   * @param file save DF in this file
   */
  def writeToParquet(file: String,
                     cols: Seq[String] = columnNames): Unit = {
    val writeColNames = cols.filter {
      column(_).csvWritable
    }

    val fields = writeColNames.map { colName => StructField(colName, column(colName).sqlType, true) }
    val rowRdd = ColumnZipper.zipAndMap(writeColNames.map(column(_))) {
      Row.fromSeq(_)
    }
    val rowsSchemaRdd = new SQLContext(sc).applySchema(rowRdd, StructType(fields))
    rowsSchemaRdd.saveAsParquetFile(file)
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

    val col = nameToColumn.get(colName)
    nameToColumn(colName) = that
    if (!col.isEmpty) {
      println(s"Replaced Column: ${colName}")
      that.index = col.get.index
    } else {
      println(s"New Column: ${colName}")
      val colIndex = indexToColumnName.size
      indexToColumnName.put(colIndex, colName)
      that.index = colIndex
    }
    that.name = colName
    that.df = Some(this)
    rowsRddCached = None //invalidate cached rows
    //FIXME: incremental computation of rows
  }

  /**
   * add/replace a column in this df
   * @param colIndex index of column, will be overwritten if it exists
   * @param that column to add to this df
   */
  //FIXME: unit test
  def setColumn(colIndex: Int, that: Column[Any]) = {
    require(that.df.isEmpty && that.index == -1 && colIndex < columnCount && column(that.name) == null)

    val col = Some(column(colIndex))
    nameToColumn(that.name) = that
    if (!col.isEmpty) {
      println(s"Replaced Column: ${col.get.name}")
    } else {
      println(s"New Column: ${that.name}")
    }
    indexToColumnName.put(colIndex, that.name)
    that.index = colIndex
    that.df = Some(this)
    rowsRddCached = None //invalidate cached rows
    //FIXME: incremental computation of rows
  }

  /**
   * get multiple columns identified by names
   * @param colNames names of columns
   */
  def columnsByNames(colNames: Seq[String]) = colNames.map(column(_))

  /**
   * get columns by ranges of numeric indices. invalid indices are silently ignored.
   * FIXME: solo has to be "5 to 5" for now, should be just "5"
   * @param indexRanges sequence of ranges like List(1 to 5, 13 to 15)
   */
  def columnsByRanges(indexRanges: Seq[Range]) = for (
      indexRange <- indexRanges;
      index <- indexRange
      if (indexToColumnName(index) != null)
    ) yield column(indexToColumnName(index))


  /**
   * get columns by sequence of numeric indices
   * @param indices Sequence of indices like List(1, 3, 13)
   */
  def columnsByIndices(indices: Seq[Int]) = {
    require(indices.forall {
      indexToColumnName.contains(_)
    })

    val indexRanges = indices.map { i => i to i }
    columnsByRanges(indexRanges)
  }

  /**
   * wrapper on filter to create a new DF from filtered RDD
   * @param cond a predicate to filter on e.g. df("price") > 10
   */
  def where(cond: Predicate): DF = {
    if (filterWithRowStrategy) {
      fromRows(filterRowStrategy(cond))
    } else {
      filter(filterColumnStrategy(cond))
    }
  }

  /**
   * slice the df to get a contiguous subset of rows
   * @param indexRange row index range
   * @return a new df
   */
  def rowsByRange(indexRange: Range): DF = {
    val pred = (rowIndexCol >= indexRange.start.toDouble) &&
      (rowIndexCol <= indexRange.end.toDouble)

    where(pred)
  }

  /**
   * filter using given RDD of Booleans
   * @param filtration RDD of Booleans. True retains corresponding row in the output
   * @return a new df
   */
  def filter(filtration: RDD[Boolean]) = {
    val cols = nameToColumn.clone
    for (i <- 0 until columnCount) {
      def applyFilter[T: ClassTag](in: RDD[T]) = {
        ColumnZipper.filterBy(in, filtration)
      }
      val colName = indexToColumnName(i)
      val col = cols(colName)

      col.colType match {
        case ColType.Double => cols(colName) = Column(sc, applyFilter(col.doubleRdd), i)
        case ColType.Float => cols(colName) = Column(sc, applyFilter(col.floatRdd), i)
        case ColType.Short => cols(colName) = Column(sc, applyFilter(col.shortRdd), i)
        case ColType.String => cols(colName) = Column(sc, applyFilter(col.stringRdd), i)
        case ColType.ArrayOfString => cols(colName) = Column(sc, applyFilter(col.arrayOfStringRdd), i)
        case ColType.ArrayOfDouble => cols(colName) = Column(sc, applyFilter(col.arrayOfDoubleRdd), i)
        case ColType.MapOfStringToFloat => cols(colName) = Column(sc, applyFilter(col.mapOfStringToFloatRdd), i)
        case ColType.Undefined => {
          throw new RuntimeException(
            s"ERROR: Column type ${col.colType} undefined for column $colName in DF $toString")
        }
      }
    }
    println(cols)
    new DF(sc, cols, indexToColumnName.clone,
      name = s"filtered_$name",
      options = options)
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
    println(cond)
    val zippedColRdd = ColumnZipper.makeRows(this, cond.colSeq)
    val colMap = new HashMap[Int, Int] //FIXME: switch to a fast map here
    var i = 0
    cond.colSeq.foreach { colIndex =>
      colMap(colIndex) = i
      i += 1
    }
    zippedColRdd.map(cols => cond.checkWithColStrategy(cols, colMap))
  }

  /**
   * using same schema as current DF, create a new one with different rows
   * used by filter() and other methods
   * @param rows
   * @return
   */
  private def fromRows(rows: RDD[Array[Any]]) = {
    val newColumns = columns().map { col =>
      val i = col.index
      val colName = col.name

      col.colType match {
        case ColType.Double => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Double] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.Float => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Float] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.Short => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Short] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.String => {
          val colRdd = rows.map { row => row(i).asInstanceOf[String] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.ArrayOfString => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Array[String]] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.ArrayOfDouble => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Array[Double]] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.MapOfStringToFloat => {
          val colRdd = rows.map { row => row(i).asInstanceOf[Map[String, Float]] }
          Column(sc, colRdd, -1, colName)
        }

        case ColType.Undefined =>
          throw new Exception(s"fromRows: Unhandled type ${col.tpe}")
      }
    }

    DF.fromColumns(sc, newColumns, s"${name}_fromRows", options)
  }

  /**
   * rename columns
   * @param columns a map of old name to new name
   * @param inPlace true to modify this df, false to create a new one
   */
  def rename(columns: Map[String, String], inPlace: Boolean = true) = {
    val df = if (inPlace == false) new DF(sc, nameToColumn.clone, indexToColumnName.clone,
      name = name, options = options)  //FIXME: clone the columns too
    else this

    columns.foreach {
      case (oldName, newName) =>
        val col = df.nameToColumn.remove(oldName)
        if (!col.isEmpty) {
          val (i, _) = df.indexToColumnName.find(x => x._2 == oldName).get
          df.indexToColumnName.put(i, newName)
          df.nameToColumn.put(newName, col.get)
          println(s"${oldName}[${i}] --> ${newName}")
        } else {
          println(s"Did not find column ${oldName}, skipping it")
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
    val x = rowsRdd.map { row => if (CountHelper.countNaN(row) > 0) 1 else 0 }
    x.reduce {
      _ + _
    }
  }

  /**
   * number of columns that have NA
   */
  def countColsWithNA = {
    columns().map { col => if (col.hasNA) 1 else 0 }.reduce { _ + _ }
  }

  /**
   * create a new DF after removing all rows that had NAs
   */
  def dropNA(rowStrategy: Boolean = filterWithRowStrategy) = {
    if (rowStrategy) {
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
      nameToColumn(_).index
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
  def aggregate[U: ru.TypeTag, V: ru.TypeTag, W: ru.TypeTag](aggByCol: String,
                                                       aggedCol: String,
                                                       aggtor: Aggregator[U, V, W]) = {
    aggregateWithColumnStrategy(List(aggByCol), aggedCol, aggtor)
  }

  /**
   * aggregate one column after grouping by another
   */
  private def aggregateWithColumnStrategy[V: ru.TypeTag, C: ru.TypeTag, W: ru.TypeTag]
    (aggdByCols: Seq[String], aggdCol: String, aggtor: Aggregator[V, C, W]) = {
    require(column(aggdCol).tpe =:= ru.typeOf[V])

    val wtpe = ru.typeTag[W]
    implicit val vClassTag = SparkUtil.typeTagToClassTag[V]

    val aggedRdd = keyBy(aggdByCols, aggdCol).asInstanceOf[RDD[(List[Any], V)]]
      .combineByKey(aggtor.convert, aggtor.mergeValue, aggtor.mergeCombiners)

    val keyCols = aggdByCols.map(column(_))
    val valueColRdd = column(aggdCol).getRdd[V]

    val x = ColumnZipper.zipAndMapSideCombine(
      keyCols, valueColRdd, KeyMaker.makeKey, aggtor.convert, aggtor.mergeValue)

    aggedRdd.cache()

    // columns of key
    var i = 0
    val aggdByColsInResult = aggdByCols.map { aggdByCol =>
      val j = i
      val col = column(aggdByCol).colType match {
        case ColType.Double => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Double]
          }.cache()
          Column(sc, col1)
        }
        case ColType.Float => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Float]
          }.cache()
          Column(sc, col1)
        }
        case ColType.Short => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Short]
          }.cache()
          Column(sc, col1)
        }
        case ColType.String => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[String]
          }.cache()
          Column(sc, col1)
        }
        case ColType.ArrayOfString => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Array[String]]
          }.cache()
          Column(sc, col1)
        }
        case ColType.ArrayOfDouble => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Array[Double]]
          }.cache()
          Column(sc, col1)
        }
        case ColType.MapOfStringToFloat => {
          val col1 = aggedRdd.map { case (k, v) =>
            k(j).asInstanceOf[Map[String, Float]]
          }.cache()
          Column(sc, col1)
        }
        case ColType.Undefined => {
          throw new RuntimeException(s"ERROR: Undefined column type while aggregating ${aggdByCol}")
        }
      }

      i += 1
      col.name = aggdByCol
      col
    }

    // finalize the aggregations and add column of that
    val aggdColumnInResult = if (wtpe.tpe =:= ru.typeOf[Double]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }(SparkUtil.typeTagToClassTag[W]).asInstanceOf[RDD[Double]]
      Column(sc, col1)
    } else if (wtpe.tpe =:= ru.typeOf[String]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }(SparkUtil.typeTagToClassTag[W]).asInstanceOf[RDD[String]]
      Column(sc, col1)
    } else if (wtpe.tpe =:= ru.typeOf[Array[String]]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }(SparkUtil.typeTagToClassTag[W]).asInstanceOf[RDD[Array[String]]]
      Column(sc, col1)
    } else if (wtpe.tpe =:= ru.typeOf[mutable.Map[String, Float]]) {
      val col1 = aggedRdd.map { case (k, v) =>
        aggtor.finalize(v)
      }(SparkUtil.typeTagToClassTag[W]).asInstanceOf[RDD[mutable.Map[String, Float]] ]
      Column(sc, col1)
    } else {
      throw new IllegalArgumentException(s"ERROR: aggregated result of type $wtpe is not supported")
    }

    aggdColumnInResult.name = aggdCol

    val newDFName = s"${name}/${aggdCol}_aggby_${aggdByCols.mkString(";")}"

    DF.fromColumns(sc, aggdByColsInResult :+ aggdColumnInResult, newDFName, options)
  }

  private def keyBy(colNames: Seq[String], valueCol: String) = {
    val columns = colNames.map {
      nameToColumn(_).index
    }
    ColumnZipper.makeList(this, columns)
  }.zip(column(valueCol).rdd)

  /**
   * aggregate multiple columns after grouping by multiple other columns
   * @param aggByCols sequence of columns to group by
   * @param aggedCol sequence of columns to be aggregated
   * @param aggtor implementation of Aggregator
   * @tparam U
   * @tparam V
   * @return new DF with first column aggByCol and second aggedCol
   */
  def aggregate[U: ru.TypeTag, V: ru.TypeTag, W: ru.TypeTag](aggByCols: Seq[String],
                                                       aggedCol: Seq[String],
                                                       aggtor: Aggregator[U, V, W]) = {
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
  def pivot(keyCol: String,
            pivotByCol: String,
            pivotedCols: List[String] = nameToColumn.keys.toList): DF = {
    val grped = groupBy(keyCol)
    val pivotValues = column(pivotByCol).colType match {
      case ColType.String => column(pivotByCol).distinct.collect.asInstanceOf[Array[String]]
      case ColType.Double => column(pivotByCol).distinct.collect.asInstanceOf[Array[Double]]
        .map {
        _.toString
      }
      case _ => {
        println(s"Pivot does not yet support columns ${column(pivotByCol)}")
        null
      }
    }

    val pivotIndex = nameToColumn.getOrElse(pivotByCol, null).index

    /*
        filter pivot by and key column from output
     */
    val cleanedPivotedCols = pivotedCols.map {
      nameToColumn(_).index
    }
      .filter { colIndex => colIndex != nameToColumn(pivotByCol).index && colIndex != nameToColumn(keyCol).index }

    val newDf = DF(sc, s"${name}_${keyCol}_pivot_${pivotByCol}", options)

    /*
        add key column to output
     */
    column(keyCol).colType match {
      case ColType.String => newDf.setColumn(s"$keyCol",
          Column(sc, grped.map(_._1.asInstanceOf[String])))
      case ColType.Double =>  newDf.setColumn(s"$keyCol",
          Column(sc, grped.map(_._1.asInstanceOf[Double])))
      case _ => {
        println(s"Pivot does not yet support columns ${column(keyCol)}")
      }
    }

    /*
        add pivoted columns
     */
    pivotValues.foreach { pivotValue =>
      val grpSplit = new PivotHelper(grped, pivotIndex, pivotValue).get

      cleanedPivotedCols.foreach { pivotedColIndex =>
        nameToColumn(indexToColumnName(pivotedColIndex)).colType match {
          case ColType.Double => {
            val newColRdd = grpSplit.map {
              case (k, v) =>
                if (v.isEmpty) Double.NaN else v.head(pivotedColIndex).asInstanceOf[Double]
            }
            newDf.setColumn(s"D_${indexToColumnName(pivotedColIndex)}@$pivotByCol==$pivotValue",
              Column(sc, newColRdd.asInstanceOf[RDD[Double]]))
          }
          case ColType.String => {
            val newColRdd = grpSplit.map {
              case (k, v) =>
                if (v.isEmpty) "" else v.head(pivotedColIndex).asInstanceOf[String]
            }
            newDf.setColumn(s"S_${indexToColumnName(pivotedColIndex)}@$pivotByCol==$pivotValue",
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
    require(nameToColumn.contains(colName))
    nameToColumn.get(colName).get
  }

  /**
   * get a column identified by numerical index
   * @param colIndex index of the column
   */
  def column(colIndex: Int) = {
    require(indexToColumnName.contains(colIndex))
    nameToColumn.get(indexToColumnName(colIndex)).get
  }

  def delete(colName: String): Unit = {
    require(nameToColumn.contains(colName))

    val col = nameToColumn.remove(colName).get
    val name = indexToColumnName.remove(col.index).get
    assert(name == colName)

    reIndexCols()
  }

  def reIndexCols(): Unit = {
    val cols = indexToColumnName.toSeq.sortBy(_._1)
    indexToColumnName.clear()
    (0 until columnCount).foreach { i =>
      indexToColumnName(i) = cols(i)._2
    }
  }

  /**
   * group by a column, uses a lot of memory. try to use aggregate(By) instead if possible
   */
  def groupBy(colName: String) = {
    keyBy(colName, rowsRdd).groupByKey
  }

  private def keyBy(colName: String, valueRdd: RDD[Array[Any]]) = {
    nameToColumn(colName).rdd
  }.zip(valueRdd)

  /**
   * print brief description of the DF
   */
  def describe(detail: Boolean = false): Unit = {
    nameToColumn.foreach {
      case (name, col) =>
        println(s"${name}\t\t\t${col.colType}\t\t\t${col.index}")
        if (detail) println(col.toString)
    }
  }

  /**
   * print upto numRows x numCols elements of the dataframe
   */
  def list(numRows: Int = 10, numCols: Int = 10): Unit = {
    println(s"Dimensions: $rowCount x $columnCount")
    val nRows = Math.min(numRows, rowCount)
    val nCols = Math.min(numCols, columnCount)
    val someCols = (0 until nCols).toList.map { colIndex => nameToColumn(indexToColumnName(colIndex)) }
    val someRows = someCols.map { col => col.head(nRows.toInt).toList }.transpose
    someRows.foreach { row =>
      println(row.mkString("| ", "  ", " |"))
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
      val cleanedColName = if (nameToColumn.contains(colName)) {
        println(s"**WARN**: Duplicate column ${colName} renamed")
        colName + "_"
      } else {
        colName
      }
      nameToColumn.put(cleanedColName, null)
      indexToColumnName.put(i, cleanedColName)
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
   * @param sc The spark context
   * @param inFile Full path to the input CSV/TSV file. If running on cluster, it should be accessible on all nodes
   * @param separator The field separator e.g. ',' for CSV file
   * @param nParts number of parts to process in parallel
   */
  def apply(sc: SparkContext, inFile: String, separator: Char, nParts: Int, options: Options): DF = {
    if (FileUtils.isDir(inFile)(sc)) fromCSVDir(sc, inFile, ".*", true, separator, nParts, options)
    else fromCSVFile(sc, inFile, separator, nParts, options = options)
  }

  def fromColumns(sc: SparkContext,
                  cols: Seq[Column[Any]],
                  dfName: String = "fromColumns", options: Options = Options()): DF = {
    require(!cols.isEmpty && cols.map(_.name).toSet.size == cols.map(_.name).toList.length)
    cols.foreach { col => require(col.rdd.partitions.length == cols.head.rdd.partitions.length) }
    val df = DF(sc, s"$dfName", options)
    cols.foreach { col =>
      df.setColumn(col.name, col)
    }
    df
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
               separator: Char,  //FIXME: move to options
               nParts: Int = 0,
               schema: Map[String, ColType.EnumVal] = Map(),
               options: Options = Options()): DF = {

    val file = if (nParts == 0) sc.textFile(inFile) else sc.textFile(inFile, nParts)

    // parse header line
    val firstLine = file.first
    val header = new LineCsvReader(fieldSep = separator,
      ignoreLeadingSpace = options.csvParsingOpts.ignoreLeadingWhitespace,
      ignoreTrailingSpace = options.csvParsingOpts.ignoreTrailingWhiteSpace,
      quote = options.csvParsingOpts.quoteChar,
      escape = options.csvParsingOpts.escapeChar
    ).parseLine(firstLine)
    println(s"Found ${header.size} columns in header")

    // parse data lines
    val dataLines = file.mapPartitionsWithIndex({
      case (partitionIndex, iter) => if (partitionIndex == 0) iter.drop(1) else iter
    }, true)
    dataLines.setName(s"data/$inFile")

    val rows = dataLines.mapPartitionsWithIndex({
      case (split, iter) => {
        new BulkCsvReader(iter, split,
          fieldSep = separator,
          ignoreLeadingSpace = options.csvParsingOpts.ignoreLeadingWhitespace,
          ignoreTrailingSpace = options.csvParsingOpts.ignoreTrailingWhiteSpace,
          quote = options.csvParsingOpts.quoteChar,
          escape = options.csvParsingOpts.escapeChar,
          numFields = header.size,
          badLinePolicy = options.lineParsingOpts.badLinePolicy,
          fillValue = options.lineParsingOpts.fillValue)
      }
    }, true).persist(options.perfTuningOpts.storageLevel)

    // FIXME: see if this is a lot faster with an rdd.unzip function
    val colRdds = for (i <- 0 until header.length) yield {
      rows.map { row => row(i) }.persist(options.perfTuningOpts.storageLevel)
    }

    val columns = header.zip(colRdds).map { case (colName, colRdd) =>
      val i = header.indexOf(colName)
      val t = if (schema.contains(colName)) {
        schema.get(colName).get
      } else if (options.numberParsingOpts.enable) {
        if (options.schemaGuessingOpts.fastSamplingEnable) {
          val firstFewRows = rows.take(options.schemaGuessingOpts.fastSamplingSize)
          SchemaUtils.guessTypeByFirstFew(firstFewRows.map { row => row(i) }, options.numberParsingOpts)
        } else {
          SchemaUtils.guessType(sc, colRdds(i))
        }
      } else {
        ColType.String
      }

      colRdd.setName(s"$t/$inFile/${colName}")
      println(s"Column: ${colName} \t\t\tGuessed Type: ${t}")

      val col = if (t == ColType.Double) {
        Column.asDoubles(
          sc, colRdd, -1, options.perfTuningOpts.storageLevel, options.numberParsingOpts, colName, None)
      } else {
        Column(sc, colRdd, -1, colName)
      }

      col
    }

    DF.fromColumns(sc, columns, s"fromCSVFile: ${inFile}", options)
  }

  /**
   * create DF from a parquet file. Schema should be flat and contain numeric and string types only(for now)
   * @param sc The spark context
   * @param inFile Full path to the input file. If running on cluster, it should be accessible on all nodes
   */
  def fromParquet(sc: SparkContext,
                  inFile: String,
                  options: Options = Options()): DF = {
    val sparkDF = new SQLContext(sc).parquetFile(inFile)
    require(sparkDF.schema.fields.forall { field =>
        field.dataType == DoubleType ||
        field.dataType == FloatType ||
        field.dataType == StringType ||
        field.dataType == IntegerType ||
        field.dataType == ShortType
    })

    val cols = sparkDF.schema.fields.map { field =>
      val i = sparkDF.schema.fields.indexOf(field)
      SparkUtil.sqlToColType(field.dataType) match {
        case ColType.Double =>
          val rdd = sparkDF.rdd.map { row => row(i).asInstanceOf[Double] }
          rdd.setName(s"$inFile/parquet-double/${field.name}")
          Column(sc, rdd, -1, field.name)
        case ColType.String =>
          val rdd = sparkDF.rdd.map { row => row(i).asInstanceOf[String] }
          rdd.setName(s"$inFile/parquet-string/${field.name}")
          Column(sc, rdd, -1, field.name)
        case _ => throw new Exception("Unsupported type")
      }
    }

    DF.fromColumns(sc, cols, "fromParquetFile: $inFile", options)
  }


  /**
   * create a DF given column names and vectors of columns(not rows)
   */
  def apply(sc: SparkContext,
            header: Vector[String],
            vec: Vector[Vector[Any]],
            dfName: String,
            options: Options): DF = {
    require(header.length == vec.length, "Shape mismatch")
    require(vec.map(_.length).toSet.size == 1, "Not a Vector of Vectors")

    val cols = header.zip(vec).map { case(colName, col) =>
      col(0) match {
        case c: Double =>
          println(s"Column: ${colName} Type: Double")
          Column(sc, sc.parallelize(col.asInstanceOf[Vector[Double]]), -1, colName)

        case c: String =>
          println(s"Column: ${colName} Type: String")
          Column(sc, sc.parallelize(col.asInstanceOf[Vector[String]]), -1, colName)
      }
    }

    DF.fromColumns(sc, cols, dfName, options)
  }

  /**
   * relational-like join two DFs
   */
  def join(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val newDf = DF(sc, s"${left.name}-join-${right.name}", left.options)
    val joinedRows = joinRdd(sc, left, right, on, how)
    val firstRow = joinedRows.first

    def leftValue(row: (Any, (Array[Any], Array[Any]))) = row._2._1
    def rightValue(row: (Any, (Array[Any], Array[Any]))) = row._2._2

    /*
     * if the two DFs being joined have columns with same names, prefix them with
     * left_ and right_ in the joined DF
     */
    def joinedColumnName(name: String, start: Int) = {
      val collision = !left.nameToColumn.keys.toSet.intersect(right.nameToColumn.keys.toSet).isEmpty
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
        val newColName = joinedColumnName(curDf.indexToColumnName(origIndex), start)

        val column = curDf.column(origIndex).colType match {
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
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Array[String]] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.ArrayOfDouble => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Array[Double]] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.Short => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Short] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.MapOfStringToFloat => {
            val colRdd = joinedRows.map { row => partGetter(row)(origIndex).asInstanceOf[Map[String, Float]] }
            Column(curDf.sc, colRdd, joinedIndex)
          }
          case ColType.Undefined => {
            throw new RuntimeException(s"ERROR: Column type UNKNOWN for ${newColName}")
          }
        }

        newDf.nameToColumn.put(newColName, column)
        newDf.indexToColumnName(joinedIndex) = newColName
      }
    }

    addCols(left, 0, leftValue)
    addCols(right, left.columnCount, rightValue)

    newDf
  }

  /**
   * make an empty DF
   */
  def apply(sc: SparkContext, name: String, options: Options) = {
    new DF(sc, new HashMap[String, Column[Any]], new HashMap[Int, String],
      name = name,
      options = options)
  }

  def joinRdd(sc: SparkContext, left: DF, right: DF, on: String, how: JoinType.JoinType = JoinType.Inner) = {
    val leftWithKey = left.nameToColumn(on).rdd.zip(left.rowsRdd)
    val rightWithKey = right.nameToColumn(on).rdd.zip(right.rowsRdd)
    leftWithKey.join(rightWithKey)
  }

  def compareSchema(a: DF, b: DF) = {
    val aColNames = a.columnNames.sorted
    val bColNames = b.columnNames.sorted
    val sameNames = aColNames.sameElements(bColNames)

    val aColTypes = a.columns().map { col => col.colType }
    val bColTypes = b.columns().map { col => col.colType }
    val sameTypes = aColTypes.sameElements(bColTypes)

    if (!(sameNames && sameTypes)) {
      println(s"${a.name} and ${b.name} differ in schema")
      println(s"a: $aColNames")
      println(s"b: $bColNames")
      println(s"a: $aColTypes")
      println(s"b: $bColTypes")
      // val bt = bColNames.zip(bColTypes)
      // val at = aColNames.zip(aColTypes)
      // (0 until columnCount).foreach{ x => if(at(x)._2 != bt(x)._2) println(s"${at(x)} -- ${bt(x)} == $x") }
      false
    } else {
      println(s"${a.name} and ${b.name} have the same schema")
      true
    }
  }

  def union(sc: SparkContext, dfs: List[DF]) = {
    require(dfs.size > 0)
    require(dfs.tail.forall { df => compareSchema(dfs.head, df) })

    val cols = dfs.head.nameToColumn.clone
    val df = dfs.head

    def unionRdd[T: ClassTag](rdds: List[RDD[T]]) = new UnionRDD[T](sc, rdds)

    for (i <- 0 until dfs.head.columnCount) {
      val unionCol = dfs.head.column(i).colType match {
        case ColType.Double => {
          val cols = dfs.map { df => df.column(i).doubleRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Float => {
          val cols = dfs.map { df => df.column(i).floatRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.String => {
          val cols = dfs.map { df => df.column(i).stringRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.ArrayOfString => {
          val cols = dfs.map { df => df.column(i).arrayOfStringRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.ArrayOfDouble => {
          val cols = dfs.map { df => df.column(i).arrayOfDoubleRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Short => {
          val cols = dfs.map { df => df.column(i).shortRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.MapOfStringToFloat => {
          val cols = dfs.map { df => df.column(i).mapOfStringToFloatRdd }
          Column(sc, unionRdd(cols), i)
        }
        case ColType.Undefined => {
          throw new RuntimeException(s"ERROR: Column type UNKNOWN for ${df.column(i)}")
        }
      }
      val colName = df.indexToColumnName(i)
      cols(colName) = unionCol
    }

    new DF(df.sc, cols, df.indexToColumnName.clone,
      name = s"filtered_${df.name}",
      options = df.options)
  }
}
