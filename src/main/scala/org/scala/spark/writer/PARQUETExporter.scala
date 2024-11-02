package org.scala.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object PARQUETExporter {

  /**
   * Utility to write a DataFrame to a specified folder path.
   *
   * @param dataFrame   The Spark DataFrame to be written.
   * @param path        The folder path where the DataFrame will be saved.
   * @param partitionBy An optional sequence of columns by which to partition the DataFrame.
   * @param options     A map of key-value pairs to specify additional options for writing.
   * @param saveMode    The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists".
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).parquet(path)
  }

  /**
   * Utility to write a DataFrame to a specified path.
   *
   * @param dataFrame The Spark DataFrame to be written.
   * @param path      The folder path where the DataFrame will be saved.
   * @param saveMode  The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists".
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    PARQUETExporter.write(dataFrame, path, Map[String, String](), saveMode)
  }

  /**
   * Utility to write a DataFrame to a specified folder path.
   *
   * @param dataFrame The Spark DataFrame to be written.
   * @param path      The folder path where the DataFrame will be saved.
   * @param options   A map of key-value pairs for additional options when writing the DataFrame.
   * @param saveMode  The mode for saving the DataFrame, which can be "overwrite", "append", "ignore", or "errorIfExists".
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).parquet(path)
  }
}
