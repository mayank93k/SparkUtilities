package org.scala.spark.reader

import com.google.cloud.spark.bigquery.BigQueryDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object BQParser {

  /**
   * Reads data from BigQuery tables using specified Spark options, project name, dataset name, and table name.
   *
   * Note: Reading from views is enabled by default.
   *
   * @param sparkSession The active Spark session
   * @param project      The BigQuery project name (e.g., 'wmt-cill-dev')
   * @param dataset      The dataset name (e.g., 'cillSchema')
   * @param table        The table name (e.g., 'cillSchema')
   * @param options      Additional options for reading the table
   * @param viewsEnabled Whether to enable reading from views (default is true)
   * @return The resulting DataFrame
   */
  def read(sparkSession: SparkSession, project: String, dataset: String, table: String,
           options: Map[String, String] = Map[String, String](), viewsEnabled: String = "true"): DataFrame = {
    read(sparkSession, options ++ Map("viewsEnabled" -> viewsEnabled), project + "." + dataset + "." + table)
  }

  /**
   * Reads data from BigQuery tables using specified Spark BigQuery options.
   *
   * For additional Spark BigQuery connector options, refer to:
   * [[https://github.com/GoogleCloudDataproc/spark-bigquery-connector#properties Spark BigQuery Options]]
   *
   * Note: External tables are not currently supported:
   * [[https://stackoverflow.com/a/71919866]]
   *
   * @param sparkSession The active Spark session
   * @param options      Spark BigQuery options
   * @param table        The name of the table to read
   * @return The resulting DataFrame
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], table: String): DataFrame = {
    sparkSession.read.options(options).bigquery(table)
  }
}
