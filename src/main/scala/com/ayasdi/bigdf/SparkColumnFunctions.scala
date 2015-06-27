package org.apache.spark.sql

class SparkColumnFunctions(self: Column) {
  def dataType = self.expr.dataType
}
