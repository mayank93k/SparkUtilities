package org.scala.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object JSONExporter {

  /**
   * Utility to write a DataFrame to a specified folder path with optional partitioning.
   *
   * @param dataFrame   The Spark DataFrame to be written
   * @param path        The folder path where the DataFrame will be saved
   * @param partitionBy An optional sequence of columns to partition the data by
   * @param options     A map of key-value pairs for configuring the write operation (e.g., file format, compression)
   * @param saveMode    The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).json(path)
  }

  /**
   * Utility to write a DataFrame to a specified path.
   *
   * @param dataFrame The Spark DataFrame to be written
   * @param path      The folder path where the DataFrame will be saved
   * @param saveMode  The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    JSONExporter.write(dataFrame, path, Map[String, String](), saveMode)
  }

  /**
   * Utility to write a DataFrame to a specified folder path with optional configurations.
   *
   * @param dataFrame The Spark DataFrame to be written
   * @param path      The folder path where the DataFrame will be saved
   * @param options   A map of key-value pairs for additional write options (e.g., compression, format)
   * @param saveMode  The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).json(path)
  }
}
