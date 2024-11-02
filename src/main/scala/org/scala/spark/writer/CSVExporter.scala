package org.scala.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object CSVExporter {

  /**
   * Utility to write a DataFrame to the specified folder path.
   *
   * @param dataFrame   The DataFrame to be written
   * @param path        The folder path where the DataFrame should be saved
   * @param partitionBy A sequence of column names to partition the output data by,
   *                    improving query performance on large datasets
   * @param options     A map of key-value pairs to configure the data writing,
   *                    such as compression settings or format-specific options
   * @param saveMode    The save mode to use when writing the DataFrame, such as
   *                    "overwrite", "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).csv(path)
  }

  /**
   * Utility to write a DataFrame to the specified path.
   *
   * @param dataFrame The DataFrame to be saved
   * @param path      The folder path where the DataFrame will be written
   * @param saveMode  The save mode to use, such as "overwrite", "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    CSVExporter.write(dataFrame, path, Map[String, String](), saveMode)
  }

  /**
   * Utility to write a DataFrame to the specified folder path.
   *
   * @param dataFrame The DataFrame to be written
   * @param path      The folder path where the DataFrame will be saved
   * @param options    A map of key-value pairs to configure the write operation,
   *                  such as specifying file format or compression settings
   * @param saveMode  The save mode to use when writing the DataFrame, which can be "overwrite",
   *                  "append", "ignore", or "errorIfExists"
   */
  def write(dataFrame: DataFrame, path: String, options: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(options).csv(path)
  }
}
