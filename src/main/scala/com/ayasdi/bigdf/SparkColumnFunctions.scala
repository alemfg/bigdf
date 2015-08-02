package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression

class SparkColumnFunctions(self: Column) {
  def dataType = self.expr.dataType
  def expr = self.expr
}

object SparkColumnFunctions {
  def apply(colName: String): Column = new Column(colName)

  def apply(expr: Expression): Column = new Column(expr)

  def unapply(col: Column): Option[Expression] = Some(col.expr)
}
