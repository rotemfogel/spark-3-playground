package me.rotemfo.common

import com.google.common.base.Splitter
import me.rotemfo.common.Schema.colNameUrlParamsRow
import me.rotemfo.common.UserAgentUtils.parseUserAgent
import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{get_json_object, lit, udf, when}
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.jackson.Serialization
import org.json4s.{Extraction, Formats, NoTypeHints}

object functions {
  def emptyStringToNull(str: String): Option[String] = {
    Option(str).getOrElse("").trim match {
      case ""     => None
      case "[]"   => None
      case "null" => None
      case _      => Some(str.trim)
    }
  }

  val udfEmptyStringToNull: UserDefinedFunction = udf(emptyStringToNull _)

  def fromJson[T](s: String)(implicit m: Manifest[T]): T = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints).skippingEmptyValues
    org.json4s.jackson.parseJson(s).extract[T]
  }

  /** convert object to JSON string
    *
    * @param a any object
    * @return JSON string
    */
  def toJson(a: Any): String = {
    implicit val formats: Formats = Serialization.formats(NoTypeHints).skippingEmptyValues
    compact(render(Extraction.decompose(a)))
  }

  /** merge two maps
    *
    * @param m1 first map
    * @param m2 second map
    * @tparam K Map key
    * @tparam V Map value
    * @return merged map
    */
  def mergeMaps[K, V](m1: Map[K, V], m2: Map[K, V]): Map[K, V] = {
    if (m2.isEmpty) m1
    else (m1.keySet ++ m2.keySet).map(k => if (m2.contains(k)) (k, m2(k)) else (k, m1(k))).toMap
  }

  def getJsonObjectNullSafe(c: Column, path: String): Column = {
    when(c.isNotNull, get_json_object(c, path))
      .otherwise(lit(null))
  }

  def userAgentParser(userAgentString: String, ua: Broadcast[UserAgentAnalyzer]): Option[String] = {
    parseUserAgent(userAgentString, ua.value)
  }

  def userAgentParserBroadcast(
      userAgentColumn: Column,
      ua: Broadcast[UserAgentAnalyzer]
  ): Column = {
    val udfUserAgentParser: UserDefinedFunction = udf((x: String) => userAgentParser(x, ua))
    udfUserAgentParser(userAgentColumn)
  }

  private val camelRegex = "[A-Z\\d]".r

  /** Takes a camel cased string and returns a snake case one
    *
    * Example:
    * camelToUnderscores("thisIsA1Test") == "this_is_a_1_test"
    *
    * @param name a camel case string
    * @return a sname case string
    */
  def camelToSnake(name: String): String = camelRegex.replaceAllIn(
    name,
    { m =>
      "_" + m.group(0).toLowerCase()
    }
  )

  private val snakeRegex = "_([a-z\\d])".r

  /** Takes an snake case string and returns a camel cased one
    *
    * Example:
    * underscoreToCamel("this_is_a_1_test") == "thisIsA1Test"
    *
    * @param name a string in snake case
    * @return a String in camel case
    */
  def snakeToCamel(name: String): String = snakeRegex.replaceAllIn(
    name,
    { m =>
      m.group(1).toUpperCase()
    }
  )

  import scala.collection.JavaConverters._

  private final def splitter(text: String): String = {
    val map = Splitter.on('&').trimResults.withKeyValueSeparator('=').split(text).asScala
    toJson(map)
  }

  private def urlParamsToJsonString(urlParams: String): String = {
    splitter(urlParams.split("\\?").last)
  }

  def urlParamsParser(urlParms: String): Option[String] = {
    try {
      if (urlParms == null || urlParms.trim.length == 1 || urlParms.trim.isEmpty) None
      else Some(urlParamsToJsonString(urlParms))
    } catch {

      case _: Exception => Some(s"""{"$colNameUrlParamsRow":"$urlParms"}""")
    }
  }

  val udfUrlParamsParser: UserDefinedFunction = udf(urlParamsParser _)

  /** Filter fields from a given Json column
    * recommended for spark 2.4.x and a lot of fieldsToDrop
    *
    * @param dataFrame    - the data frame
    * @param jsonColumn   - the JSON Column
    * @param fieldsToDrop - the fileds to drop
    * @param spark        - sparkSQLContext
    * @return StructType
    */
  def getJsonSchema(dataFrame: DataFrame, jsonColumn: Column, fieldsToDrop: Array[String])(implicit
      spark: SQLContext
  ): StructType = {
    val dsCol: Dataset[String] = dataFrame.select(jsonColumn.as[String](Encoders.STRING))
    if (fieldsToDrop.nonEmpty)
      StructType(
        spark.read.json(dsCol).schema.filterNot(field => fieldsToDrop.contains(field.name))
      )
    else
      spark.read.json(dsCol).schema
  }
}
