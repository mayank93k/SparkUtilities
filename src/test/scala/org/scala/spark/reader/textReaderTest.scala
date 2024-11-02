package org.scala.spark.reader

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class textReaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("TextReaderTest")
      .master("local[*]")
      .getOrCreate()
  }


  override def afterAll(): Unit = {
    spark.stop()
  }

  "textReader" should "read a text file into a DataFrame" in {
    // Given
    val textData = Seq("John Doe,30", "Jane Doe,25").mkString("\n")

    // Create a temporary directory for the text file
    val tempDir = java.nio.file.Files.createTempDirectory("textReaderTest").toString
    val tempTextPath = s"$tempDir/test.txt"

    // Write text data to the file
    java.nio.file.Files.write(java.nio.file.Paths.get(tempTextPath), textData.getBytes)

    // When
    val resultDf = textReader.read(spark, tempTextPath)

    // Define expected output
    val expectedRows = Seq(
      Row("John Doe,30"),
      Row("Jane Doe,25")
    )

    // Then
    resultDf.collect() should contain theSameElementsAs expectedRows
  }

  it should "handle a malformed text file and check for corrupt records" in {
    // Given
    // Create a temporary directory for the malformed text file
    val tempDir = java.nio.file.Files.createTempDirectory("textReaderTest").toString
    val tempTextPath = s"$tempDir/malformed.txt"

    // Write malformed data to the file
    val malformedData = "malformed line without a delimiter"
    java.nio.file.Files.write(java.nio.file.Paths.get(tempTextPath), malformedData.getBytes)

    // When
    val resultDf = textReader.read(spark, tempTextPath)

    // Check the count of rows in the resulting DataFrame
    val totalRecordsCount = resultDf.count()

    // Then
    // Since the input is malformed, we expect the resulting DataFrame to have 1 row
    totalRecordsCount should be(1L) // We expect 1 row, but check its content

    // Validate the content of the DataFrame
    val firstRow = resultDf.collect()
    firstRow.length should be(1) // Ensure there's one row
    firstRow(0).getString(0) shouldEqual "malformed line without a delimiter" // Ensure it contains the malformed line
  }

}

