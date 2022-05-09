package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.catalyst.util.FunctionUtils
import org.apache.spark.sql.types.{
  AbstractDataType,
  DataType,
  IntegerType,
  StringType
}

case class IntPadding(number: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {
  override def child: Expression = number

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

  override def dataType: DataType = StringType

  override def nullSafeEval(n: Any): Any = {
    FunctionUtils.intPadding(n.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val dtu = FunctionUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, sd => s"$dtu.intPadding($sd)")
  }

  override def prettyName: String = "int_padding"
}
