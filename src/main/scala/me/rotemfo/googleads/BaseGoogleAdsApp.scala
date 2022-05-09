package me.rotemfo.googleads

import me.rotemfo.common.functions.camelToSnake
import me.rotemfo.common.sql.DataFrameExSql
import me.rotemfo.common.{ParquetReaderApplication, ReaderConfig}
import me.rotemfo.googleads.schema.CommonSchema.{accountIdColumn, colDate, colLabels, dateColumn}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

abstract class BaseGoogleAdsApp extends ParquetReaderApplication {

  case class CompoundColumn(snakeName: String, field: StructField)

  /** schema to define which columns to convert from Null to Zero
    * NULL numeric values to 0 converted to input column data type
    * must override in child class !
    *
    * @return StructType
    */
  protected def nullToZeroSchema: StructType

  /** explicitly get the nullToZeroSchema (for testing purposes)
    *
    * @return StructType
    */
  def getNullToZeroSchema: StructType = nullToZeroSchema

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  protected def schema: StructType

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  protected def fieldMappingFilters: Seq[String]

  /** list of columns to partition by the dataframe
    *
    * @return Sequence of Strings
    */
  protected def partitionByColumns: Seq[String] = Seq(accountIdColumn, dateColumn)

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return
    */
  protected def specificTransformFn: DataFrame => DataFrame = (df: DataFrame) => df

  /** convert NULL numeric values to 0 converted to input column data type
    *
    * @param schema the schema containing columns
    * @return DataFrame
    */
  private def nullToZero(schema: StructType): DataFrame => DataFrame =
    (dataFrame: DataFrame) =>
      schema.fields.foldLeft(dataFrame) { case (df, c) =>
        df.withColumn(
          c.name,
          when(col(c.name).isNull, lit(0).cast(c.dataType))
            .otherwise(col(c.name))
        )
      }

  /** according to the fieldMappingFilters,
    * append `${root}_` prefix before all campaign data
    * due to duplicate column issues.
    *
    * for example:
    * adGroupSchema contains 2 structs:
    * root
    * |-- campaign: struct (nullable = true)
    * |    |-- id: string (nullable = true)
    * |    |-- name: string (nullable = true)
    * |-- adGroup: struct (nullable = true)
    * |    |-- id: string (nullable = true)
    * |    |-- name: string (nullable = true)
    * when flattening the schema, we get duplicate columns (from campaign.id and adGroup.id)
    * the result of this function will be:
    * root
    * |-- campaign_id: string (nullable = true)
    * |-- campaign_name: string (nullable = true)
    * |-- id: string (nullable = true)
    * |-- name: string (nullable = true)
    */
  private val fieldMapFunction: (String, StructField) => CompoundColumn =
    (rootKey: String, compoundColumn: StructField) => {
      val snakeName: String = camelToSnake(compoundColumn.name)
      val colName: String =
        if (fieldMappingFilters.contains(rootKey)) s"${camelToSnake(rootKey)}_$snakeName"
        else snakeName
      CompoundColumn(colName, compoundColumn)
    }

  /** generate field mapping between root level and the leaf columns
    *
    * @return Map between root element and its leaf columns
    */
  private def getFieldsMap: Map[String, Array[CompoundColumn]] =
    schema.fields
      .map((f: StructField) =>
        f.name -> f.dataType
          .asInstanceOf[StructType]
          .fields
          .map((c: StructField) => fieldMapFunction(f.name, c))
      )
      .toMap

  /** flatten schema and transform column names from camelCase to snakeCase
    *
    * @param fieldsMap a mapping from rootKey to its columns
    * @return DataFrame
    */
  private def transformFields(
      fieldsMap: Map[String, Array[CompoundColumn]]
  ): DataFrame => DataFrame =
    (dataFrame: DataFrame) => {
      fieldsMap
        .foldLeft(dataFrame) { case (df, c) =>
          val rootKey: String = c._1
          c._2.foldLeft(df) { case (ds, cc) =>
            if (
              ds.schema.fields.exists((f: StructField) =>
                rootKey.equals(f.name) &&
                  f.dataType.isInstanceOf[StructType] &&
                  f.dataType
                    .asInstanceOf[StructType]
                    .fields
                    .exists((_: StructField).name.equals(cc.field.name))
              )
            )
              ds.withColumn(
                cc.snakeName,
                col(rootKey).getItem(cc.field.name).cast(cc.field.dataType)
              )
            else
              ds.withColumn(cc.snakeName, lit(null).cast(cc.field.dataType))
          }
        }
    }

  /** generate a select list based on the field mapping
    *
    * @param fieldsMap the field mapping map
    * @return List of Strings
    */
  protected def generateSelectList(fieldsMap: Map[String, Array[CompoundColumn]]): Seq[String] =
    fieldsMap.values.flatten
      .map((c: CompoundColumn) =>
        if (c.snakeName.equals(colDate)) c.copy(snakeName = dateColumn)
        else c
      )
      .map((_: CompoundColumn).snakeName)
      .toSeq ++ Seq(accountIdColumn)

  protected def handleLabels: DataFrame => DataFrame =
    (df: DataFrame) => df.transform(handleLabels(colLabels))

  /** handle labels column transformation
    *
    * @return DataFrame
    */
  protected def handleLabels(labelsColumn: String): DataFrame => DataFrame =
    (df: DataFrame) => {
      val tmpLabels = s"tmp_$labelsColumn"
      df.withColumn(
        tmpLabels,
        when(col(labelsColumn).isNotNull, to_json(col(labelsColumn)))
          .otherwise(lit(null).cast(StringType))
      ).drop(labelsColumn)
        .withColumnRenamed(tmpLabels, labelsColumn)
    }

  /** main entry point for Spork code
    *
    * @param p     config parameters
    * @param spark SQLContext
    */
  override def invoke(implicit p: ReaderConfig, spark: SQLContext): Unit = {
    val fieldsMap: Map[String, Array[CompoundColumn]] = getFieldsMap
    val selectList: Seq[String]                       = generateSelectList(fieldsMap)

    spark.read
      .parquet(p.inputLocation.get)
      .transform(transformFields(fieldsMap))
      .transform(nullToZero(nullToZeroSchema))
      .withColumn(dateColumn, to_date(col(colDate), "yyyy-MM-dd"))
      .transform(specificTransformFn) // specific job transformation
      .select(selectList.map(col): _*)
      .repairColumns
      .coalesce(1)
      .write
      .option("partitionOverwriteMode", "dynamic")
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionByColumns: _*)
      .parquet(p.outputLocation.get)
  }
}
