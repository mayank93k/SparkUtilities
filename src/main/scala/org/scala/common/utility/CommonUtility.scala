package org.scala.common.utility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scala.common.logger.Logging

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.Try

object CommonUtility extends Logging {
  /**
   * Converts a date represented as a string in "yyyyMMdd" format to a `Timestamp`.
   *
   * This method takes an integer date in string format (e.g., "20231102" for November 2, 2023)
   * and converts it to a long date format represented by a `Timestamp`.
   * The input date should be valid; otherwise, an exception may be thrown.
   *
   * @param date Input date in string format (e.g., "20231102")
   * @return DateTime in timestamp format as `Option[Timestamp]` Returns `Some(Timestamp)` if conversion is successful,
   *         or `None` if the input date format is invalid.
   */
  def dateTimeInMilliConvertor(date: String): Option[Timestamp] = {
    Option(date).flatMap { d =>
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss.SSS")
      Try(Timestamp.valueOf(LocalDateTime.parse(d, formatter))).toOption
    }
  }

  /**
   * Retrieves a list of files in a specified directory that were created between the given start
   * and end timestamps.
   *
   * This method scans the specified directory and filters the files based on their creation
   * timestamps, returning only those files created within the defined time range.
   *
   * @param spark                         The Spark Session used to interact with the Spark environment.
   * @param path                          The directory path to search for files.
   * @param startProcessedOffsetTimestamp The start timestamp (inclusive) of the time range for filtering files.
   * @param endProcessedOffsetTimestamp   The end timestamp (exclusive) of the time range for filtering files.
   * @return A list of file paths (as Strings) that were created between the specified timestamps.
   *         The list may be empty if no files match the criteria.
   */
  def filterFilesOlderThanCreationTime(spark: SparkSession, path: Path, startProcessedOffsetTimestamp: Long, endProcessedOffsetTimestamp: Long): List[String] = {
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileItr = fileSystem.listFiles(path, true)
    val listObj = ListBuffer.empty[String]

    while (fileItr.hasNext) {
      val fileObj = fileItr.next()
      if ((fileObj.getModificationTime > startProcessedOffsetTimestamp) && (fileObj.getModificationTime <= endProcessedOffsetTimestamp)) {
        listObj += fileObj.getPath.toString
      }
    }
    listObj.toList
  }

  /**
   * Checks whether the specified path exists, is a directory, and contains files.
   *
   * @param pathString The string representation of the path to check.
   * @param hadoopConf The Hadoop Configuration used to access the filesystem.
   * @return True if the path exists, is a directory, and contains files; false otherwise.
   */
  private def isValidDirectoryWithFiles(pathString: String, hadoopConf: Configuration): Boolean = {
    if (pathExistsAndIsDirectory(pathString, hadoopConf) && directoryHasFiles(pathString, hadoopConf)) {
      true
    } else {
      false
    }
  }

  /**
   * Checks if the specified path exists and is a directory.
   *
   * @param pathString The string representation of the path to check.
   * @param hadoopConf The Hadoop Configuration used to access the filesystem.
   * @return True if the path exists and is a directory; false otherwise.
   */
  private def pathExistsAndIsDirectory(pathString: String, hadoopConf: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(pathString)
    fs.exists(path) && fs.getFileStatus(path).isDirectory
  }

  /**
   * Checks if the specified directory contains any files.
   *
   * @param pathString The string representation of the path to check.
   * @param hadoopConf The Hadoop Configuration used to access the filesystem.
   * @return True if the directory contains files; false otherwise.
   */
  private def directoryHasFiles(pathString: String, hadoopConf: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(pathString)
    fs.getContentSummary(path).getDirectoryCount.toInt != 0
  }
}
