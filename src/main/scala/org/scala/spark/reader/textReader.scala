package org.scala.spark.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

object textReader {

  /**
   * Utility to read TEXT and return data frame
   *
   * @param sparkSession This is the spark session
   * @param paths        This is the path(s) from which the files(s) need to be read
   * @return The dataframe from the given file
   */
  def read(sparkSession: SparkSession, paths: String*): DataFrame = {
    textReader.read(sparkSession, Map[String, String](), paths: _*)
  }

  /**
   * Utility to read TEXT and return data frame
   *
   * @param sparkSession This is the spark session
   * @param options      This is a map of options
   * @param paths        This is the path(s) from which the files(s) need to be read
   * @return The dataframe from the given file
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], paths: String*): DataFrame = {
    sparkSession.read.options(options).text(paths: _*)
  }
}