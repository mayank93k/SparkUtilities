package org.scala.spark.reader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVParser {

  /**
   * Utility to read CSV files and return a DataFrame.
   *
   * @param sparkSession The active Spark session
   * @param paths        The path(s) to the CSV file(s) to be read
   * @return The resulting DataFrame from the specified CSV file(s)
   */
  def read(sparkSession: SparkSession, schema: StructType, paths: String*): DataFrame = {
    CSVParser.read(sparkSession, schema, Map("header" -> "true"), paths: _*)
  }

  /**
   * Utility to read CSV files and return a DataFrame.
   *
   * @param sparkSession The active Spark session
   * @param options      A map of options for reading the CSV files
   * @param paths        The path(s) to the CSV file(s) to be read
   * @return The resulting DataFrame from the specified CSV file(s)
   */
  def read(sparkSession: SparkSession, schema: StructType, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.schema(schema).options(options).csv(paths: _*)
  }
}
