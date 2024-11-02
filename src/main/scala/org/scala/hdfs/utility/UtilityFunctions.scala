package org.scala.hdfs.utility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

object UtilityFunctions {

  /**
   * Reads the latest available path that corresponds to a numerical digit, utilizing 'org.apache.hadoop.fs.Path'
   * for path representation.
   *
   * This method expects the input path to be a numeric value that falls within the integer range.
   * If the path does not conform to this format, a NumberFormatException will be thrown.
   *
   * @param path     The numeric path represented as a string, which should be a valid integer value.
   * @param hdConfig Configuration settings for Hadoop, required to facilitate the reading of paths.
   * @throws NumberFormatException if the provided path cannot be parsed as a numeric digit or is outside the Int range.
   * @return The latest available Path object that matches the criteria.
   */
  @throws(classOf[NumberFormatException])
  def getLatestNumericPath(path: Path, hdConfig: Configuration): Path = {
    val fs = FileSystem.get(hdConfig)
    val pathMap = fs.listStatus(path).filter(_.isDirectory)
      .map(_.getPath)
      .flatMap((fileInfo: Path) => {
        Map[Int, Path](
          fileInfo.getName.replaceAll("[/:]", "").toInt -> fileInfo
        )
      }).toMap

    pathMap(pathMap.keySet.max)
  }

  /**
   * Retrieves the latest partition path where the name is a numerical digit, represented as an instance of
   * 'org.apache.hadoop.fs.Path'.
   *
   * @param basePath The base path from which to derive the numeric partition paths. This path
   *                 must be a valid numeric representation within the long range.
   * @param hdConfig Configuration settings required for the operation.
   * @tparam T The type of partition value, restricted to Int, Double, Float, or Long.
   * @throws NumberFormatException if the provided path cannot be parsed as a numerical digit.
   * @return An instance of Path representing the latest numeric partition path.
   */
  @throws(classOf[NumberFormatException])
  def fetchLatestPartitionPath[T](basePath: Path, hdConfig: Configuration)(implicit pathImplicit: PathImplicits[T]): Path = {
    val paths: Array[Path] = getPaths(basePath, hdConfig)
    val filteredPaths = paths.filter(eachPath => eachPath.getName.contains("="))
    pathImplicit.findLatestPartitionPath(filteredPaths)
  }

  /**
   * Deletes a specified path in HDFS based on the provided parameters.
   *
   * The behavior of the deletion depends on the value of the `recursive` flag:
   * - If `recursive` is **false**:
   *   - The method will delete the specified path only if it is empty.
   *     - If `recursive` is **true**:
   *   - The method will delete the specified path along with all its contents, including files and subdirectories.
   *
   * The method returns `true` if the deletion operation is successful, and `false` otherwise.
   *
   * @param path       The path to be deleted (of type `Path`).
   * @param recursive  A boolean flag indicating whether to delete the path recursively (true) or only if empty (false).
   * @param hadoopConf The Hadoop configuration object that contains necessary configurations for the operation.
   * @return `true` if the deletion was successful; otherwise, `false`.
   */
  def deleteHdfsPath(path: Path, recursive: Boolean, hadoopConf: Configuration): Boolean = {
    val fs = FileSystem.get(hadoopConf)
    fs.exists(path) && fs.delete(path, recursive)
  }

  /**
   * Retrieves unique paths with a partition value greater than the specified threshold.
   *
   * @param basePath       The numeric base path from which to derive partition paths.
   * @param hdConfig       The Hadoop Configuration object to interact with HDFS.
   * @param partitionValue The threshold partition value; paths with values greater than this will be included.
   * @param pathImplicits  Implicit conversions for the partition value type.
   * @tparam T The type of the partition value, restricted to Int, Float, Long, and Double.
   * @return A collection of unique paths that have a partition value exceeding the given threshold.
   */
  def getPathsGreaterThanPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                          (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    pathImplicits.getPathMapGreaterThanPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Retrieves a set of unique paths that have a partition value less than the specified partition value
   * within the given base path.
   *
   * @param basePath       The base path from which to retrieve partition directories.
   * @param hdConfig       The Hadoop Configuration used to interact with the filesystem.
   * @param partitionValue The partition value that serves as the exclusive upper boundary for the selection.
   * @param pathImplicits  An implicit object that provides functionality for managing partition values of type T.
   * @tparam T The type of the partition values, restricted to Int, Float, Long, and Double.
   * @return A set of Paths representing the directories that contain partition values less than
   *         the specified partitionValue.
   */
  def getPathsLessThanPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                       (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    pathImplicits.getPathMapLessThanPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Retrieves a set of unique paths that have a partition value less than or equal to the specified partition value
   * within the given base path.
   *
   * @param basePath       The base path from which to retrieve partition directories.
   * @param hdConfig       The Hadoop Configuration used to interact with the filesystem.
   * @param partitionValue The partition value that serves as the upper boundary for the selection.
   * @param pathImplicits  An implicit object that provides functionality for managing partition values of type T.
   * @tparam T The type of the partition values, restricted to Int, Float, Long, and Double.
   * @return A set of Paths representing the directories that contain partition values less than or equal to
   *         the specified partitionValue.
   */
  def getPathsLessThanEqualPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                            (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    pathImplicits.getPathMapLessThanEqualPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Retrieves a set of partition paths that are greater than or equal to the specified partition value
   * within the given base path.
   *
   * @param basePath       The base path to search for partition directories.
   * @param hdConfig       The Hadoop Configuration object used for interacting with HDFS.
   * @param partitionValue The partition value that serves as the lower boundary for the selection.
   * @param pathImplicits  An implicit object providing functionality for working with partition values of type T.
   * @tparam T The type of the partition values, restricted to Int, Double, Float, and Long.
   * @return A set of Paths representing the partition directories that have a partition value greater than or equal
   *         to the specified partitionValue.
   */
  def getPathsGreaterThanEqualPartitionValue[T](basePath: Path, hdConfig: Configuration, partitionValue: T)
                                               (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    pathImplicits.getPathMapGreaterThanEqualPartitionValue(getPaths(basePath, hdConfig), partitionValue).values.toSet
  }

  /**
   * Retrieves an array of directory paths under the specified base path.
   * This method filters the results to include only directories.
   *
   * @param basePath The base path from which to list subdirectories.
   * @param hdConfig The Hadoop Configuration object to interact with the HDFS.
   * @return An array of Paths representing the directories found under the base path.
   */
  private def getPaths(basePath: Path, hdConfig: Configuration): Array[Path] = {
    val fs = FileSystem.get(hdConfig)
    fs.listStatus(basePath).filter(_.isDirectory).map(_.getPath)
  }

  /**
   * Retrieves a set of partition paths that fall within a specified range of partition values, inclusive of the boundary values.
   *
   * This method searches the specified base path for partitions and returns only those whose values lie between
   * the provided lower and upper boundaries.
   *
   * @param basePath            The base path to search for partitions.
   * @param hdConfig            The Hadoop Configuration object for interacting with HDFS.
   * @param partitionValueStart The lower boundary of the partition value range (inclusive).
   * @param partitionValueEnd   The upper boundary of the partition value range (inclusive).
   * @param pathImplicits       An implicit object that provides functionality for working with partition values of type T.
   * @tparam T The type of the partition values, restricted to Int, Double, Float, and Long.
   * @return A set of Paths representing the partition directories that fall within the specified range.
   */
  def getPathsBetweenPartitionValues[T](basePath: Path, hdConfig: Configuration, partitionValueStart: T, partitionValueEnd: T)
                                       (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    pathImplicits.getPathMapBetweenPartitionValues(getPaths(basePath, hdConfig), partitionValueStart, partitionValueEnd).values.toSet
  }

  /**
   * Retrieves a set of partition paths that fall within a specified range of partition values, inclusive of the boundary values.
   * This method allows filtering of partition paths based on a specified key, which is used to include only those paths that
   * contain the filter string in their names.
   *
   * @param basePath            The base path to search for partitions.
   * @param hdConfig            The Hadoop Configuration object for interacting with HDFS.
   * @param partitionValueStart The lower boundary of the partition value range (inclusive).
   * @param partitionValueEnd   The upper boundary of the partition value range (inclusive).
   * @param filterKey           A string to filter partition paths, ensuring only those containing this string in their names are included.
   *                            Defaults to "=" if not provided.
   * @param pathImplicits       An implicit object that provides functionality for working with partition values of type T.
   * @tparam T The type of the partition values, restricted to Int, Double, Float, and Long.
   * @return A set of Paths representing the partition directories that fall within the specified range and meet the filtering criteria.
   */
  def getPathsBetweenPartitionValues[T](basePath: Path, hdConfig: Configuration, partitionValueStart: T, partitionValueEnd: T, filterKey: String = "=")
                                       (implicit pathImplicits: PathImplicits[T]): Set[Path] = {
    val paths: Array[Path] = getPaths(basePath, hdConfig).filter(eachPath => eachPath.getName.contains(filterKey))
    pathImplicits.getPathMapBetweenPartitionValues(paths, partitionValueStart, partitionValueEnd).values.toSet
  }
}
