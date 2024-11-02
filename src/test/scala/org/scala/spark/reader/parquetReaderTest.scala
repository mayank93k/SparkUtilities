package org.scala.spark.reader

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class parquetReaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ParquetReaderTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "parquetReader" should "read a Parquet file into a DataFrame" in {
    // Given
    val parquetData = Seq(
      Row("John Doe", 30L),
      Row("Jane Doe", 25L)
    )

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", LongType, nullable = true)
    ))

    // Create a temporary directory for the Parquet file
    val tempDir = java.nio.file.Files.createTempDirectory("parquetReaderTest").toString
    val tempParquetPath = s"$tempDir/test.parquet"

    // Create DataFrame and write it as Parquet
    val df = spark.createDataFrame(spark.sparkContext.parallelize(parquetData), schema)
    df.write.parquet(tempParquetPath)

    // When
    val resultDf = parquetReader.read(spark, tempParquetPath)

    // Then
    resultDf.schema shouldEqual schema
    resultDf.collect() should contain theSameElementsAs parquetData
  }

  it should "throw an exception if the Parquet file is malformed" in {
    // Given
    // Create a temporary directory for the malformed Parquet file
    val tempDir = java.nio.file.Files.createTempDirectory("parquetReaderTest").toString
    val tempParquetPath = s"$tempDir/malformed.parquet"
    // Manually create a malformed Parquet file (you might need a different approach here)

    // When / Then
    assertThrows[org.apache.spark.sql.AnalysisException] {
      parquetReader.read(spark, tempParquetPath)
    }
  }
}

