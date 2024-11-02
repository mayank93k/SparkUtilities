package org.scala.spark.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object csvReader {

  /**
   * Utility to read CSV and return data frame
   *
   * @param sparkSession This is the spark session
   * @param paths        This is the path(s) from which the files(s) need to be read
   * @return The dataframe from the given file
   */
  def read(sparkSession: SparkSession, schema: StructType, paths: String*): DataFrame = {
    csvReader.read(sparkSession, schema, Map("header" -> "true"), paths: _*)
  }

  /**
   * Utility to read CSV and return data frame
   *
   * @param sparkSession This is the spark session
   * @param options      This is a map of options
   * @param paths        This is the path(s) from which the files(s) need to be read
   * @return The dataframe from the given file
   */
  def read(sparkSession: SparkSession, schema: StructType, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.schema(schema).options(options).csv(paths: _*)
  }
}
