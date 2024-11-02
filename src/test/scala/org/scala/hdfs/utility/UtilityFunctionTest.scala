package org.scala.hdfs.utility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilityFunctionTest extends AnyFlatSpec with Matchers {
  "getLatestNumericPath" should "return the latest path based on numerical directory names" in {
    // Setup
    val hdConfig: Configuration = new Configuration()
    val path = new Path("src/test/resources/outputPath/test/")

    // Execute
    val latestPath: Path = UtilityFunctions.getLatestNumericPath(path, hdConfig)
    val expectedPath = new Path("/Users/mayank/Documents/Projects/SparkUtilities/src/test/resources/outputPath/test/234")
    // Verify that the latest path is correctly returned
    latestPath.toString should be("file:" + expectedPath) // Expected path
  }

  it should "throw NumberFormatException for non-numerical directory names" in {
    // Setup for paths that are not numerical
    val hdConfig: Configuration = new Configuration()
    val path = new Path("src/test/resources/outputPath/")

    // Expecting a NumberFormatException
    assertThrows[NumberFormatException] {
      UtilityFunctions.getLatestNumericPath(path, hdConfig)
    }
  }
}
