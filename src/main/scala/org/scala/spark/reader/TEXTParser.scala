package org.scala.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object TEXTParser {

  /**
   * Utility to read text files and return a DataFrame.
   *
   * @param sparkSession The active Spark session used for reading data
   * @param paths        The path(s) to the text file(s) to be read. Supports both local file systems
   *                     and distributed file systems like HDFS or S3.
   * @return A DataFrame where each line of the text file(s) is represented as a single string in the "value" column
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    TEXTParser.read(sparkSession, Map[String, String](), paths: _*)
  }

  /**
   * Utility to read text files and return a DataFrame.
   *
   * @param sparkSession The active Spark session used for reading data
   * @param options      A map of options to configure text file reading, such as specifying delimiters or
   *                     custom line separators
   * @param paths        The path(s) to the text file(s) to be read, supporting both local and
   *                     distributed file systems like HDFS, S3, or GCS
   * @return A DataFrame where each line of the text file(s) is stored as a single string in the "value" column
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).text(paths: _*)
  }
}
