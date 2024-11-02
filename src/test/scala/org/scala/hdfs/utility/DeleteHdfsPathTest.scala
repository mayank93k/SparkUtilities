package org.scala.hdfs.utility

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scala.hdfs.utility.UtilityFunctions.deleteHdfsPath
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeleteHdfsPathTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  val hadoopConf: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(hadoopConf)

  // Setup a test directory structure
  val baseDir = new Path("src/main/resources/paths/testDir")
  val emptyDir = new Path("src/main/resources/paths/testDir/emptyDir")
  val nonEmptyDir = new Path("src/main/resources/paths/testDir/nonEmptyDir")
  val testFile = new Path("src/main/resources/paths/testDir/nonEmptyDir/test.json")

  override def beforeAll(): Unit = {
    fs.mkdirs(baseDir)
    fs.mkdirs(emptyDir)
    fs.mkdirs(nonEmptyDir)
    fs.create(testFile).close()
  }

  override def afterAll(): Unit = {
    // Clean up after tests
    deleteHdfsPath(baseDir, recursive = true, hadoopConf)
  }

  "deleteHdfsPath" should "delete an empty directory when recursive is false" in {
    val result = deleteHdfsPath(emptyDir, recursive = false, hadoopConf)
    result should be(true)
    fs.exists(emptyDir) should be(false) // Ensure the directory is deleted
  }

  it should "delete a non-empty directory when recursive is true" in {
    val result = deleteHdfsPath(nonEmptyDir, recursive = true, hadoopConf)
    result should be(true)
    fs.exists(nonEmptyDir) should be(false) // Ensure the directory is deleted
  }

  it should "return false for a non-existing path" in {
    val nonExistingPath = new Path("/testDir/nonExistingDir")
    val result = deleteHdfsPath(nonExistingPath, recursive = false, hadoopConf)
    result should be(false) // Deletion should fail
  }
}
