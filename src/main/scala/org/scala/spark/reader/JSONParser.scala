package org.scala.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object JSONParser {

  /**
   * Utility to read JSON files and return a DataFrame.
   *
   * @param sparkSession The active Spark session
   * @param paths        The path(s) to the JSON file(s) to be read
   * @return The resulting DataFrame from the specified JSON file(s)
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    JSONParser.read(sparkSession, Map[String, String](), paths: _*)
  }

  /**
   * Utility to read JSON files and return a DataFrame.
   *
   * @param sparkSession The active Spark session
   * @param options      A map of options for reading the JSON files
   * @param paths        The path(s) to the JSON file(s) to be read
   * @return The resulting DataFrame from the specified JSON file(s)
   */
  def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).json(paths: _*)
  }
}
