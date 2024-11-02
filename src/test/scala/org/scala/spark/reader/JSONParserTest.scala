package org.scala.spark.reader

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JSONParserTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("JsonReaderTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "JSONParser" should "read a JSON file into a DataFrame" in {
    // Given
    val jsonData = """[{"age": 30, "name": "John Doe"}, {"age": 25, "name": "Jane Doe"}]"""

    // Create a temporary directory for the JSON file
    val tempDir = java.nio.file.Files.createTempDirectory("JSONParserTest").toString
    val tempJsonPath = s"$tempDir/test.json"
    java.nio.file.Files.write(java.nio.file.Paths.get(tempJsonPath), jsonData.getBytes)

    // When
    val resultDf = JSONParser.read(spark, tempJsonPath)

    // Define expected schema with the correct types
    val expectedSchema = StructType(Seq(
      StructField("age", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))

    // Then
    // Check schema equality ignoring field order
    resultDf.schema.fields.map(f => (f.name, f.dataType)) should contain theSameElementsAs
      expectedSchema.fields.map(f => (f.name, f.dataType))

    // Collecting results as sets to ignore order
    val expectedRows = Set(Row(30L, "John Doe"), Row(25L, "Jane Doe")) // Correct expected structure
    val actualRows = resultDf.collect().toSet // Collect rows directly as is

    actualRows shouldEqual expectedRows
  }


  it should "handle empty JSON files gracefully" in {
    // Given
    val emptyJsonData = "[]"

    // Create a temporary directory for the empty JSON file
    val tempDir = java.nio.file.Files.createTempDirectory("JSONParserTest").toString
    val tempJsonPath = s"$tempDir/empty.json"
    java.nio.file.Files.write(java.nio.file.Paths.get(tempJsonPath), emptyJsonData.getBytes)

    // When
    val resultDf = JSONParser.read(spark, tempJsonPath)

    // Then
    resultDf.isEmpty shouldBe true
  }

  it should "throw an exception if the JSON file is malformed" in {
    // Given
    val malformedJsonData = """[{"name": "John Doe", "age": 30,}""" // Malformed JSON

    // Create a temporary directory for the malformed JSON file
    val tempDir = java.nio.file.Files.createTempDirectory("JSONParserTest").toString
    val tempJsonPath = s"$tempDir/malformed.json"
    java.nio.file.Files.write(java.nio.file.Paths.get(tempJsonPath), malformedJsonData.getBytes)

    // When / Then
    val exception = intercept[org.apache.spark.sql.AnalysisException] {
      // Attempt to read the malformed JSON file
      val df = JSONParser.read(spark, tempJsonPath)
      // Trigger evaluation of the DataFrame to force Spark to read the JSON
      df.collect() // This action will evaluate the DataFrame
    }

    // Assert the type of the exception
    assert(exception.isInstanceOf[org.apache.spark.sql.AnalysisException])

    // Optionally, you can assert that the exception message contains a known error pattern
    assert(exception.getMessage.contains("JSON") || exception.getMessage.contains("_corrupt_record"))
  }

  it should "read JSON files with additional options" in {
    // Given
    val jsonData = """[{"age": 30, "name": "John Doe"}, {"age": 25, "name": "Jane Doe"}]"""

    // Create a temporary directory for the JSON file
    val tempDir = java.nio.file.Files.createTempDirectory("JSONParserTest").toString
    val tempJsonPath = s"$tempDir/test.json"
    java.nio.file.Files.write(java.nio.file.Paths.get(tempJsonPath), jsonData.getBytes)

    // When
    val options = Map("multiline" -> "true")
    val resultDf = JSONParser.read(spark, options, tempJsonPath)

    // Define expected schema
    val expectedSchema = StructType(Seq(
      StructField("age", LongType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))

    // Then
    resultDf.schema shouldEqual expectedSchema
    resultDf.collect() should contain theSameElementsAs Seq(
      Row(30L, "John Doe"), // Keep LongType for age
      Row(25L, "Jane Doe")
    )
  }
}

