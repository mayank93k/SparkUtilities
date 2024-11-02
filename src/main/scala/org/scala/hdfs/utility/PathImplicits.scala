package org.scala.hdfs.utility

import org.apache.hadoop.fs.{InvalidPathException, Path}

/**
 * Trait to restrict any method to only accept defined types.
 *
 * The type parameter `T` can only be one of the types defined in [[PathImplicits]] and is provided as implicits.
 *
 * @tparam T The type parameter restricted to `Int`, `Double`, `Float`, and `Long`.
 */
sealed trait PathImplicits[T] {
  def getPartitionValuePathMap(paths: Array[Path]): Map[T, Path]

  def findLatestPartitionPath(paths: Array[Path]): Path

  def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: T): Map[T, Path]

  def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: T, partitionValueEnd: T): Map[T, Path]
}

object PathImplicits {
  /**
   * Implicit object for handling operations where the type is restricted to `Int`.
   *
   * This object extends `PathImplicits[Int]` and provides specific implementations
   * for methods that require `Int` as the type parameter.
   */
  implicit object PathInInt extends PathImplicits[Int] {
    /**
     * Finds the latest partition path from an array of paths.
     *
     * This method takes an array of `Path` objects, extracts the partition values, and identifies the path corresponding
     * to the highest partition value. It assumes that the paths are formatted in a way that allows for partition value extraction.
     *
     * @param paths An array of `Path` objects from which the latest partition path is to be determined.
     *              Each path should conform to the expected naming convention that includes partitioning.
     * @return The `Path` that represents the latest partition based on the maximum partition value found in the provided
     *         paths.
     * @throws NoSuchElementException if the provided paths array is empty or if no valid partition paths can be extracted.
     */
    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      // Get a map of partition values to their corresponding paths
      val partitionValuePathMap = getPartitionValuePathMap(paths)

      // Find the path corresponding to the maximum partition value
      val latestPartitionValue = partitionValuePathMap.keySet.max
      partitionValuePathMap(latestPartitionValue)
    }

    /**
     * Retrieves a map of paths whose partition values are less than a specified value.
     *
     * This method filters the provided array of `Path` objects to create a mapping of partition values to their
     * corresponding paths. It only includes those paths whose partition values are less than the specified `partitionValue`.
     *
     * @param paths          An array of `Path` objects to be examined for partition values.
     * @param partitionValue The threshold partition value; only paths with partition values less than this will be
     *                       included in the returned map.
     * @return A map where the keys are partition values that are less than the specified `partitionValue`, and the values
     *         are the corresponding `Path` objects.
     */
    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      // Get a map of partition values to their corresponding paths
      val partitionValuePathMap = getPartitionValuePathMap(paths)

      // Filter the map to include only entries with partition values less than the specified value
      partitionValuePathMap.filter { case (value, _) =>
        value < partitionValue
      }
    }

    /**
     * Retrieves a map of paths whose partition values are greater than a specified value.
     *
     * This method filters the provided array of `Path` objects to create a mapping of partition values to their
     * corresponding paths. It only includes those paths whose partition values are greater than the specified `partitionValue`.
     *
     * @param paths          An array of `Path` objects to be examined for partition values.
     * @param partitionValue The threshold partition value; only paths with partition values greater than this will be
     *                       included in the returned map.
     * @return A map where the keys are partition values that are greater than the specified `partitionValue`, and the
     *         values are the corresponding `Path` objects.
     */
    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      // Get a map of partition values to their corresponding paths
      val partitionValuePathMap = getPartitionValuePathMap(paths)

      // Filter the map to include only entries with partition values greater than the specified value
      partitionValuePathMap.filter { case (value, _) =>
        value > partitionValue
      }
    }

    /**
     * Retrieves a map of paths whose partition values are less than or equal to a specified value.
     *
     * This method filters the provided array of `Path` objects to create a mapping of partition values to their
     * corresponding paths. It includes paths whose partition values are less than or equal to the specified `partitionValue`.
     *
     * @param paths          An array of `Path` objects to be examined for partition values.
     * @param partitionValue The threshold partition value; only paths with partition values less than or equal to this
     *                       will be included in the returned map.
     * @return A map where the keys are partition values that are less than or equal to the specified `partitionValue`,
     *         and the values are the corresponding `Path` objects.
     */
    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      // Get a map of partition values to their corresponding paths
      val partitionValuePathMap = getPartitionValuePathMap(paths)

      // Filter the map to include only entries with partition values less than or equal to the specified value
      partitionValuePathMap.filter { case (value, _) =>
        value <= partitionValue
      }
    }

    /**
     * Creates a mapping of partition values to their corresponding paths from an array of `Path` objects.
     *
     * This method extracts the partition value from each `Path` and constructs a map where the keys are the partition
     * values (as integers) and the values are the corresponding `Path` objects. If multiple paths have the same partition
     * value, only the last encountered path will be retained in the map.
     *
     * @param paths An array of `Path` objects from which to extract partition values.
     * @throws InvalidPathException if unable to extract a valid partition value from a path.
     * @return A map where the keys are partition values (as integers) and the values are the corresponding `Path` objects.
     */
    override def getPartitionValuePathMap(paths: Array[Path]): Map[Int, Path] = {
      // Create a map from partition values to their corresponding paths
      paths.flatMap { path: Path =>
        // Extract the partition value and map it to the path
        Map(extractPartitionValue(path).toInt -> path)
      }.toMap
    }

    /**
     * Retrieves a mapping of partition values to their corresponding paths for those paths with partition values greater
     * than or equal to the specified partition value.
     *
     * This method filters the paths to include only those where the extracted partition value meets or exceeds the
     * provided `partitionValue`. It constructs a map where the keys are the partition values (as integers) and the values
     * are the corresponding `Path` objects. If multiple paths have the same partition value, only the last encountered path
     * will be retained in the map.
     *
     * @param paths          An array of `Path` objects from which to extract partition values.
     * @param partitionValue The partition value to compare against.
     * @throws InvalidPathException if unable to extract a valid partition value from a path.
     * @return A map where the keys are partition values (as integers) greater than or equal to the specified `partitionValue`,
     *         and the values are the corresponding `Path` objects.
     */
    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Int): Map[Int, Path] = {
      // Filter the partition value path map to return paths with partition values greater than or equal to the specified value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValue
      }
    }

    /**
     * Retrieves a mapping of partition values to their corresponding paths for those paths with partition values that
     * lie within the specified range (inclusive).
     *
     * This method filters the paths to include only those where the extracted partition values fall between the
     * `partitionValueStart` and `partitionValueEnd` parameters. It constructs a map where the keys are the partition
     * values (as integers) and the values are the corresponding `Path` objects. If multiple paths have the same partition
     * value, only the last encountered path will be retained in the map.
     *
     * @param paths               An array of `Path` objects from which to extract partition values.
     * @param partitionValueStart The lower bound of the partition value range (inclusive).
     * @param partitionValueEnd   The upper bound of the partition value range (inclusive).
     * @throws InvalidPathException if unable to extract a valid partition value from a path.
     * @return A map where the keys are partition values (as integers) that lie between `partitionValueStart` and
     *         `partitionValueEnd`, inclusive, and the values are the corresponding `Path` objects.
     */
    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Int, partitionValueEnd: Int): Map[Int, Path] = {
      // Ensure that the start value is less than or equal to the end value
      require(partitionValueStart <= partitionValueEnd)

      // Filter the partition value path map to return paths within the specified range
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValueStart && value <= partitionValueEnd
      }
    }

  }

  /**
   * Implicit object for handling operations where the type is restricted to `Double`.
   *
   * This object extends `PathImplicits[Double]` and provides specific implementations
   * for methods that require `Double` as the type parameter.
   */
  implicit object PathInDouble extends PathImplicits[Double] {
    /**
     * Finds the latest partition path from the given array of paths.
     *
     * This method extracts the partition values from the provided paths and determines the path associated with the
     * maximum partition value. It is assumed that the partition values are numerical, and the method will throw an
     * exception if any path does not contain a valid partition value.
     *
     * @param paths An array of `Path` objects from which to determine the latest partition path.
     * @throws InvalidPathException if unable to extract a valid partition value from any of the paths.
     * @return The `Path` object corresponding to the latest partition value.
     */
    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      // Retrieve a map of partition values to their corresponding paths
      val pathMap = getPartitionValuePathMap(paths)

      // Find the path associated with the maximum partition value
      val latestPartitionValue = pathMap.keySet.max
      pathMap(latestPartitionValue)
    }

    /**
     * Retrieves a map of paths with partition values less than the specified value.
     *
     * This method processes the given array of paths to extract their partition values and returns a map where each entry
     * consists of a partition value that is less than the specified `partitionValue`. The keys of the map are the partition
     * values, and the values are the corresponding `Path` objects.
     *
     * @param paths          An array of `Path` objects from which to extract partition values.
     * @param partitionValue The partition value to compare against.
     * @return A map containing partition values less than the specified `partitionValue` along with their
     *         corresponding `Path` objects.
     */
    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      // Retrieve the map of partition values to their corresponding paths
      val partitionValueMap = getPartitionValuePathMap(paths)

      // Filter the map to include only paths with partition values less than the specified value
      partitionValueMap.filter { case (value, _) => value < partitionValue }
    }

    /**
     * Constructs a map of partition values and their corresponding paths.
     *
     * This method processes the provided array of paths to extract the partition values and creates a map where each entry
     * consists of a partition value as the key and the associated `Path` object as the value. The partition value is
     * extracted from each path and is expected to be convertible to a `Double`.
     *
     * @param paths An array of `Path` objects from which to extract partition values.
     * @return A map where each key is a partition value (as a `Double`) and each value is the corresponding `Path` object.
     *         If a path cannot be converted to a partition value, it will not be included in the resulting map.
     */
    override def getPartitionValuePathMap(paths: Array[Path]): Map[Double, Path] = {
      // Convert the array of paths to a map of partition values and their corresponding paths
      paths.flatMap { path: Path =>
        // Create a map entry for each path using its extracted partition value
        Map(extractPartitionValue(path).toDouble -> path)
      }.toMap
    }

    /**
     * Retrieves a map of partition values and their corresponding paths for partition values greater than the specified threshold.
     *
     * This method filters the provided array of paths to construct a map that includes only those paths whose partition
     * values exceed the given `partitionValue`. The partition values are extracted from each path and are expected to be
     * convertible to a `Double`.
     *
     * @param paths          An array of `Path` objects from which to extract and filter partition values.
     * @param partitionValue The threshold value; only paths with partition values greater
     *                       than this value will be included in the resulting map.
     * @return A map where each key is a partition value (as a `Double`) and each value is the corresponding `Path` object,
     *         including only those paths with partition values greater than the specified `partitionValue`.
     */
    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      // Retrieve the partition value path map and filter for entries greater than the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value > partitionValue
      }
    }

    /**
     * Retrieves a map of partition values and their corresponding paths for partition values less than or equal to the
     * specified threshold.
     *
     * This method filters the provided array of paths to construct a map that includes only those paths whose partition
     * values are less than or equal to the given `partitionValue`. The partition values are extracted from each path and
     * are expected to be convertible to a `Double`.
     *
     * @param paths          An array of `Path` objects from which to extract and filter partition values.
     * @param partitionValue The threshold value; only paths with partition values less than or equal to this value will be
     *                       included in the resulting map.
     * @return A map where each key is a partition value (as a `Double`) and each value is the corresponding `Path` object,
     *         including only those paths with partition values less than or equal to the specified `partitionValue`.
     */
    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      // Retrieve the partition value path map and filter for entries less than or equal to the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value <= partitionValue
      }
    }

    /**
     * Retrieves a map of partition values and their corresponding paths for partition values greater than or equal to the
     * specified threshold.
     *
     * This method filters the provided array of paths to construct a map that includes only those paths whose partition values
     * are greater than or equal to the given `partitionValue`. The partition values are extracted from each path and are
     * expected to be convertible to a `Double`.
     *
     * @param paths          An array of `Path` objects from which to extract and filter partition values.
     * @param partitionValue The threshold value; only paths with partition values greater
     *                       than or equal to this value will be included in the resulting map.
     * @return A map where each key is a partition value (as a `Double`) and each value is the corresponding `Path` object,
     *         including only those paths with partition values greater than or equal to the specified `partitionValue`.
     */
    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Double): Map[Double, Path] = {
      // Retrieve the partition value path map and filter for entries greater than or equal to the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValue
      }
    }

    /**
     * Retrieves a map of partition values and their corresponding paths for partition values that fall between the specified
     * start and end thresholds.
     *
     * This method filters the provided array of paths to construct a map that includes only those paths whose partition values
     * are greater than or equal to `partitionValueStart` and less than or equal to `partitionValueEnd`. The partition values are
     * extracted from each path and are expected to be convertible to a `Double`.
     *
     * @param paths               An array of `Path` objects from which to extract and filter partition values.
     * @param partitionValueStart The inclusive lower threshold; only paths with partition values greater than or equal to this
     *                            value will be included.
     * @param partitionValueEnd   The inclusive upper threshold; only paths with partition values less than or equal to this value
     *                            will be included.
     * @return A map where each key is a partition value (as a `Double`) and each value is the corresponding `Path` object,
     *         including only those paths with partition values between the specified `partitionValueStart` and `partitionValueEnd`.
     */
    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Double, partitionValueEnd: Double): Map[Double, Path] = {
      require(partitionValueStart <= partitionValueEnd, "Start partition value must be less than or equal to end partition value.")

      // Retrieve the partition value path map and filter for entries within the specified range
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValueStart && value <= partitionValueEnd
      }
    }
  }

  /**
   * Implicit object for handling operations where the type is restricted to `Float`.
   *
   * This object extends `PathImplicits[Float]` and provides implementations
   * tailored for handling `Float` values in operations related to paths.
   */
  implicit object PathInFloat extends PathImplicits[Float] {
    /**
     * Finds the latest partition path from the provided array of paths based on partition values.
     *
     * This method constructs a mapping of partition values to their corresponding paths and returns the path associated with the
     * maximum partition value.
     *
     * @param paths An array of `Path` objects representing the available partition paths.
     * @throws NoSuchElementException if no valid paths are provided or if no paths contain partition values.
     * @return The `Path` object that corresponds to the latest (maximum) partition value.
     */
    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      // Create a map of partition values to paths
      val pathMap = getPartitionValuePathMap(paths)

      // Get the path corresponding to the maximum partition value
      val latestPartitionValue = pathMap.keySet.max
      pathMap(latestPartitionValue)
    }

    /**
     * Constructs a map of partition values to their corresponding paths from the provided array of paths.
     *
     * This method extracts the partition value from each path using the `extractPartitionValue` method and maps it to the path itself.
     *
     * @param paths An array of `Path` objects representing the available partition paths.
     * @return A map where the keys are partition values of type `Float` and the values are the corresponding `Path` objects.
     * @throws NumberFormatException if any path does not contain a valid partition value.
     */
    override def getPartitionValuePathMap(paths: Array[Path]): Map[Float, Path] = {
      paths.flatMap { path: Path =>
        // Create a mapping of the extracted partition value to the path
        Map(extractPartitionValue(path).toFloat -> path)
      }.toMap
    }

    /**
     * Filters the partition value paths to return those with partition values that are less than the specified threshold.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The float value to compare against.
     * @return A map where the keys are partition values of type `Float` that are less than the specified `partitionValue`,
     *         and the values are the corresponding `Path` objects.
     */
    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      // Retrieve the partition value to path mapping and filter based on the given partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value < partitionValue
      }
    }

    /**
     * Filters the partition value paths to return those with partition values that are greater than the specified threshold.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The float value to compare against.
     * @return A map where the keys are partition values of type `Float` that are greater than the specified `partitionValue`,
     *         and the values are the corresponding `Path` objects.
     */
    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      // Retrieve the partition value to path mapping and filter based on the given partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value > partitionValue
      }
    }

    /**
     * Filters the partition value paths to return those with partition values that are less than or equal to the specified threshold.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The float value to compare against.
     * @return A map where the keys are partition values of type `Float` that are less than or equal to the specified
     *         `partitionValue`, and the values are the corresponding `Path` objects.
     */
    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      // Retrieve the partition value to path mapping and filter for values less than or equal to the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value <= partitionValue
      }
    }

    /**
     * Filters the partition value paths to return those with partition values that are greater than or equal to the specified threshold.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The float value to compare against.
     * @return A map where the keys are partition values of type `Float` that are greater than or equal to the specified
     *         `partitionValue`, and the values are the corresponding `Path` objects.
     */
    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Float): Map[Float, Path] = {
      // Retrieve the partition value to path mapping and filter for values greater than or equal to the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValue
      }
    }

    /**
     * Filters the partition value paths to return those with partition values that are between the specified start and end
     * thresholds (inclusive).
     *
     * @param paths               An array of `Path` objects representing the available partition paths.
     * @param partitionValueStart The starting float value for the range (inclusive).
     * @param partitionValueEnd   The ending float value for the range (inclusive).
     * @return A map where the keys are partition values of type `Float` that fall between the specified start and end values,
     *         and the values are the corresponding `Path` objects.
     * @throws IllegalArgumentException if the start value is greater than the end value.
     */
    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Float, partitionValueEnd: Float): Map[Float, Path] = {
      // Ensure the start partition value is less than or equal to the end partition value
      require(partitionValueStart <= partitionValueEnd)

      // Retrieve the partition value to path mapping and filter for values within the specified range
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value >= partitionValueStart && value <= partitionValueEnd
      }
    }
  }

  /**
   * Implicit object for handling operations where the type is restricted to `Long`.
   *
   * This object extends `PathImplicits[Long]` and provides implementations
   * tailored for handling `Long` values in operations related to paths.
   */
  implicit object PathInLong extends PathImplicits[Long] {
    /**
     * Finds the latest partition path from the provided array of paths by identifying the path with the maximum partition value.
     *
     * @param paths An array of `Path` objects representing the available partition paths.
     * @return The `Path` object that corresponds to the maximum partition value.
     * @throws NoSuchElementException if no paths are provided or if paths do not contain valid partition values.
     */
    override def findLatestPartitionPath(paths: Array[Path]): Path = {
      // Create a map of partition values to paths
      val pathMap = getPartitionValuePathMap(paths)

      // Ensure there are paths available to avoid NoSuchElementException
      if (pathMap.isEmpty) {
        throw new NoSuchElementException("No valid paths provided for finding the latest partition.")
      }
      // Find and return the path corresponding to the maximum partition value
      pathMap(pathMap.keySet.max)
    }

    /**
     * Retrieves a map of paths whose partition values are less than the specified partition value.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The partition value to compare against.
     * @return A map where the keys are partition values that are less than the specified value, and the values are the
     *         corresponding `Path` objects.
     */
    override def getPathMapLessThanPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      // Retrieve the partition value to path mapping and filter for values less than the specified partition value
      getPartitionValuePathMap(paths).filter {
        case (value, _) => value < partitionValue
      }
    }

    /**
     * Extracts a map of partition values associated with their corresponding paths.
     *
     * @param paths An array of `Path` objects from which to extract partition values.
     * @return A map where the keys are the extracted partition values as `Long`, and the values are the corresponding `Path` objects.
     */
    override def getPartitionValuePathMap(paths: Array[Path]): Map[Long, Path] = {
      paths.flatMap { (path: Path) => {
        Map[Long, Path](extractPartitionValue(path).toLong -> path)
      }
      }.toMap
    }

    /**
     * Retrieves a map of paths whose partition values are greater than the specified partition value.
     *
     * @param paths          An array of `Path` objects representing the available partition paths.
     * @param partitionValue The partition value to compare against.
     * @return A map where the keys are partition values that are greater than the specified value, and the values are the
     *         corresponding `Path` objects.
     */
    override def getPathMapGreaterThanPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      // Filter the partition value path map to retain only those with values greater than the specified partitionValue
      getPartitionValuePathMap(paths).filter { case (value, _) =>
        value > partitionValue
      }
    }

    /**
     * Returns a map of paths where the partition values are less than or equal to the specified value.
     *
     * @param paths          An array of `Path` objects to filter.
     * @param partitionValue The partition value to compare against.
     * @return A map where the keys are partition values (Long) less than or equal to the given value, and the values are
     *         the corresponding `Path` objects.
     */
    override def getPathMapLessThanEqualPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      // Filter the partition value path map to retain only those with values less than or equal to the specified partitionValue
      getPartitionValuePathMap(paths).filter { case (value, _) =>
        value <= partitionValue
      }
    }

    /**
     * Returns a map of paths where the partition values are greater than or equal to the specified value.
     *
     * @param paths          An array of `Path` objects to filter.
     * @param partitionValue The partition value to compare against.
     * @return A map where the keys are partition values (Long) greater than or equal to the given value, and the values
     *         are the corresponding `Path` objects.
     */
    override def getPathMapGreaterThanEqualPartitionValue(paths: Array[Path], partitionValue: Long): Map[Long, Path] = {
      // Filter the partition value path map to retain only those with values greater than or equal to the specified partitionValue
      getPartitionValuePathMap(paths).filter { case (value, _) =>
        value >= partitionValue
      }
    }

    /**
     * Returns a map of paths where the partition values are between the specified start and end values (inclusive).
     *
     * @param paths               An array of `Path` objects to filter.
     * @param partitionValueStart The start partition value for the range.
     * @param partitionValueEnd   The end partition value for the range.
     * @return A map where the keys are partition values (Long) between the given start and end values, and the values are
     *         the corresponding `Path` objects.
     */
    override def getPathMapBetweenPartitionValues(paths: Array[Path], partitionValueStart: Long, partitionValueEnd: Long): Map[Long, Path] = {
      // Ensure that the start partition value is less than or equal to the end partition value
      require(partitionValueStart <= partitionValueEnd)

      // Filter the partition value path map to retain entries within the specified range
      getPartitionValuePathMap(paths).filter { case (value, _) =>
        value >= partitionValueStart && value <= partitionValueEnd
      }
    }
  }

  /**
   * Extracts the partition value from the provided path.
   *
   * This method analyzes the given `Path` object and attempts to extract the partition value based on the naming
   * convention used in the path structure. If the partition value cannot be extracted due to an invalid path format,
   * an `InvalidPathException` will be thrown.
   *
   * @param fileInfo The `Path` from which the partition value is to be extracted.
   * @throws InvalidPathException If the path does not conform to the expected format for extracting the partition value.
   * @return The extracted partition value as a `String`.
   */
  @throws(classOf[InvalidPathException])
  private def extractPartitionValue(fileInfo: Path): String = {
    fileInfo.getName.split("=").lift(1)
      .getOrElse(throw new InvalidPathException(fileInfo.toString, "Unable to extract partition value")).trim
  }
}