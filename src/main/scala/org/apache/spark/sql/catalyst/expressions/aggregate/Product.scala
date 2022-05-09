package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage =
    "_FUNC_(expr) - Returns the product calculated from values of a group.",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (1), (2), (3) AS tab(col);
       6
      > SELECT _FUNC_(col) FROM VALUES (NULL), (2), (3) AS tab(col);
       6
      > SELECT _FUNC_(col) FROM VALUES (NULL), (NULL) AS tab(col);
       NULL
  """,
  group = "agg_funcs",
  since = "1.0.0"
)
case class Product(child: Expression)
    extends DeclarativeAggregate
    with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function product")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _               => DoubleType
  }

  private lazy val product = AttributeReference("product", resultType)()

  private lazy val isEmpty =
    AttributeReference("isEmpty", BooleanType, nullable = false)()

  private lazy val one = Literal(1D)

  override lazy val aggBufferAttributes = resultType match {
    case _: DecimalType => product :: isEmpty :: Nil
    case _              => product :: Nil
  }

  override lazy val initialValues: Seq[Expression] = resultType match {
    case _: DecimalType => Seq(one, Literal(true, BooleanType))
    case _              => Seq(Literal(null, resultType))
  }

  override lazy val updateExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        // For decimal type, the initial value of `product` is 1. We need to keep `product` unchanged if
        // the input is null, as PRODUCT function ignores null input. The `product` can only be null if
        // overflow happens under non-ansi mode.
        val productExpr = if (child.nullable) {
          If(
            child.isNull,
            product,
            product * KnownNotNull(child).cast(resultType)
          )
        } else {
          product * child.cast(resultType)
        }
        // The buffer becomes non-empty after seeing the first not-null input.
        val isEmptyExpr = if (child.nullable) {
          isEmpty && child.isNull
        } else {
          Literal(false, BooleanType)
        }
        Seq(productExpr, isEmptyExpr)
      case _ =>
        // For non-decimal type, the initial value of `product` is null, which indicates no value.
        // We need `coalesce(product, one)` to start multiplying values. And we need an outer `coalesce`
        // in case the input is nullable. The `product` can only be null if there is no value, as
        // non-decimal type can produce overflowed value under non-ansi mode.
        if (child.nullable) {
          Seq(
            coalesce(coalesce(product, one) * child.cast(resultType), product)
          )
        } else {
          Seq(coalesce(product, one) * child.cast(resultType))
        }
    }
  }

  /**
    * For decimal type:
    * If isEmpty is false and if sum is null, then it means we have had an overflow.
    *
    * update of the sum is as follows:
    * Check if either portion of the left.sum or right.sum has overflowed
    * If it has, then the sum value will remain null.
    * If it did not have overflow, then add the sum.left and sum.right
    *
    * isEmpty:  Set to false if either one of the left or right is set to false. This
    * means we have seen atleast a value that was not null.
    */
  override lazy val mergeExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        val bufferOverflow = !isEmpty.left && product.left.isNull
        val inputOverflow = !isEmpty.right && product.right.isNull
        Seq(
          If(
            bufferOverflow || inputOverflow,
            Literal.create(null, resultType),
            // If both the buffer and the input do not overflow, just add them, as they can't be
            // null. See the comments inside `updateExpressions`: `sum` can only be null if
            // overflow happens.
            KnownNotNull(product.left) * KnownNotNull(product.right)
          ),
          isEmpty.left && isEmpty.right
        )
      case _ =>
        Seq(coalesce(coalesce(product.left, one) * product.right, product.left))
    }
  }

  /**
    * If the isEmpty is true, then it means there were no values to begin with or all the values
    * were null, so the result will be null.
    * If the isEmpty is false, then if product is null that means an overflow has happened.
    * So now, if ansi is enabled, then throw exception, if not then return null.
    * If product is not null, then return the product.
    */
  override lazy val evaluateExpression: Expression = resultType match {
    case d: DecimalType =>
      If(
        isEmpty,
        Literal.create(null, resultType),
        CheckOverflowInSum(product, d, !SQLConf.get.ansiEnabled)
      )
    case _ => product
  }
}
