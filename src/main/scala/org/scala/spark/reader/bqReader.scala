package org.scala.spark.reader

import com.google.cloud.spark.bigquery.BigQueryDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object bqReader {

  /**
   * Read the big query tables data by providing the spark big query options, project name,
   * dataset name and table name
   *
   * Note: Reading of view is enabled by default
   *
   * @param sparkSession This is the spark session
   * @param project      Big query options
   * @param dataset      project name eg: 'wmt-cill-dev'
   * @param table        dataset name eg: 'cillSchema'
   * @param options      table name eg: 'cillSchema'
   * @param viewsEnabled Enables the connector to read from views and not only tables (default is 'trues')
   * @return : Dataframe
   */
  def read(sparkSession: SparkSession, project: String, dataset: String, table: String,
           options: Map[String, String] = Map[String, String](), viewsEnabled: String = "true"): DataFrame = {
    read(sparkSession, options ++ Map("viewsEnabled" -> viewsEnabled), project + "." + dataset + "." + table)
  }

  /**
   * Read the big query tables data by providing the spark big query options
   * For more spark big query connector options, please check below page:
   * [[https://github.com/GoogleCloudDataproc/spark-bigquery-connector#properties Spark Big Query Options]]
   * External table Not supported as of now: [[https://stackoverflow.com/a/71919866]]
   *
   * @param sparkSession This is the spark session
   * @param options      Spark big query options
   * @param table        Table name like
   * @return The dataframe from the given file
   */
  private def read(sparkSession: SparkSession, options: Map[String, String], table: String): DataFrame = {
    sparkSession.read.options(options).bigquery(table)
  }
}
