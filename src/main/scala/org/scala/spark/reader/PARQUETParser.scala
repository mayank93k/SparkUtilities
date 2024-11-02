package org.scala.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object PARQUETParser {

  /**
   * Utility to read Parquet files and return a DataFrame.
   *
   * @param sparkSession The active Spark session used for reading data
   * @param paths        The path(s) to the Parquet file(s) to be read. This can include a single path or
   *                     multiple paths specified as a sequence. Supports both local and distributed file systems
   *                     like HDFS, Amazon S3, or Google Cloud Storage.
   * @return A DataFrame containing the data read from the specified Parquet file(s).
   *         The schema is automatically inferred from the Parquet file structure.
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    PARQUETParser.read(sparkSession, Map[String, String](), paths: _*)
  }

  /**
   * Utility to read Parquet files and return a DataFrame.
   *
   * @param sparkSession The active Spark session used for reading data
   * @param options      A map of options for reading the Parquet files, such as schema inference or specific filters
   * @param paths        The path(s) to the Parquet file(s) to be read, supporting both single and
   *                     multiple paths across local or distributed file systems
   * @return A DataFrame containing the data from the specified Parquet file(s), with the schema
   *         inferred from the file structure
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).parquet(paths: _*)
  }
}
