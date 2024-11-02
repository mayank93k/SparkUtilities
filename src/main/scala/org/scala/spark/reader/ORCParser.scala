package org.scala.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object ORCParser {

  /**
   * Utility to read ORC files and return a DataFrame.
   *
   * @param sparkSession The active Spark session
   * @param paths        The path(s) to the ORC file(s) to be read
   * @return The resulting DataFrame from the specified ORC file(s)
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    ORCParser.read(sparkSession, Map[String, String](), paths: _*)
  }

  /**
   * Utility to read ORC files and return a DataFrame.
   *
   * @param sparkSession Active Spark session
   * @param options      A map of options for reading the ORC files
   * @param paths        Path(s) to the ORC file(s) to be read
   * @return The resulting DataFrame from the specified ORC file(s)
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).orc(paths: _*)
  }
}
