package org.apache.spark.sql.catalyst.util

import org.apache.spark.unsafe.types.UTF8String

object FunctionUtils {

  type SQLInt = Int
  type SQLString = UTF8String

  def intPadding(number: SQLInt): SQLString = {
    UTF8String.fromString(f"$number%02d")
  }

}
