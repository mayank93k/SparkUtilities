package org.scala.spark.reader

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class CSVParserTest extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {
  // Create a SparkSession manually
  private val spark: SparkSession = SparkSession.builder()
    .appName("CsvReaderTest")
    .master("local[*]") // Use local mode for testing
    .getOrCreate()

  "read" should "read CSV files into a DataFrame with the provided schema" in {
    // Given
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", StringType, nullable = true)
    ))

    // Sample data for testing
    val csvData = Seq(
      ("John Doe", "30"),
      ("Jane Doe", "25")
    )

    // Create a DataFrame and write it to a temporary CSV file
    import spark.implicits._
    val df: DataFrame = csvData.toDF("name", "age")
    val tempDir = java.nio.file.Files.createTempDirectory("CSVParserTest").toString
    val tempCsvPath = s"$tempDir/test.csv"
    df.write.option("header", "true").csv(tempCsvPath)

    // When
    // Read the CSV file using CSVParser
    val resultDf = CSVParser.read(spark, schema, tempCsvPath)

    // Collect only the data (excluding headers) and compare
    resultDf.collect() should contain theSameElementsAs df.collect()
  }
}


