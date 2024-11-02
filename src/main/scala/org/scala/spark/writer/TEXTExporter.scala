package org.scala.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object TEXTExporter {

  /**
   * Utility to write a DataFrame to a specified folder path with optional partitioning.
   *
   * @param dataFrame   The Spark DataFrame to be written.
   * @param path        The folder path where the DataFrame will be saved.
   * @param partitionBy An optional sequence specifying the columns to partition the data by.
   * @param options     A map of key-value pairs for additional options when writing the DataFrame.
   * @param saveMode    The save mode for writing the DataFrame, such as "overwrite", "append", "ignore", or "errorIfExists".
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).text(path)
  }

  /**
   * Utility to write a DataFrame to a specified path.
   *
   * @param dataFrame The Spark DataFrame to be written.
   * @param path      The folder path where the DataFrame will be saved.
   * @param saveMode  The save mode for the DataFrame, determining how data is written to the data source (e.g., "overwrite", "append", "ignore", or "errorIfExists").
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    TEXTExporter.write(dataFrame, path, Map[String, String](), saveMode)
  }

  /**
   * Utility to write a DataFrame to a specified folder path.
   *
   * @param dataFrame The Spark DataFrame to be written.
   * @param path      The folder path where the DataFrame will be saved.
   * @param options   A map of key-value pairs for additional configuration options for the write operation.
   * @param saveMode  The save mode for the DataFrame, which defines how the data will be written to the data source (e.g., "overwrite", "append", "ignore", or "errorIfExists").
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).text(path)
  }
}
