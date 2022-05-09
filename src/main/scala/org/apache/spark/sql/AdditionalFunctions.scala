package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, IntPadding}
import org.apache.spark.sql.catalyst.expressions.aggregate.Product

object AdditionalFunctions {
  private def withExpr(expr: Expression): Column = Column(expr)

  def intPadding(col: Column): Column = withExpr {
    IntPadding(col.expr)
  }

  def product(e: Column): Column =
    Column(Product(e.expr).toAggregateExpression(isDistinct = false))
}
