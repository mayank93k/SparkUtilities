package org.scala.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}

object jsonWriter {

  /**
   * Utility to write data frame to a folder path specified.
   *
   * @param dataFrame   Spark data frame
   * @param path        Folder Path
   * @param partitionBy Partitioning Sequence
   * @param options     Key Value Pair
   * @param saveMode    Save mode of dataframe to a data source
   */
  def write(dataFrame: DataFrame, path: String, partitionBy: Seq[String], options: Map[String, String] = Map(),
            saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    dataFrame.write.mode(saveMode).options(options).partitionBy(partitionBy: _*).json(path)
  }

  /**
   * Utility to write data frame to a path specified
   *
   * @param dataFrame Spark data frame
   * @param path      Folder Path
   * @param saveMode  Save mode of dataframe to a data source
   */
  def write(dataFrame: DataFrame, path: String, saveMode: SaveMode): Unit = {
    jsonWriter.write(dataFrame, path, Map[String, String](), saveMode)
  }

  /**
   * Utility to write data frame to a folder path specified
   *
   * @param dataFrame Spark data frame
   * @param path      Folder Path
   * @param option    Key Value Pair
   * @param saveMode  Save mode of dataframe to a data source
   */
  def write(dataFrame: DataFrame, path: String, option: Map[String, String], saveMode: SaveMode): Unit = {
    dataFrame.write.mode(saveMode).options(option).json(path)
  }
}
