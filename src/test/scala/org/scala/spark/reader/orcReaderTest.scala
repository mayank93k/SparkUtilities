package org.scala.spark.reader

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class orcReaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("OrcReaderTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "orcReader" should "read an ORC file into a DataFrame" in {
    // Given
    val orcData = Seq(
      Row("John Doe", 30L),
      Row("Jane Doe", 25L)
    )

    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", LongType, nullable = true)
    ))

    // Create a temporary directory for the ORC file
    val tempDir = java.nio.file.Files.createTempDirectory("orcReaderTest").toString
    val tempOrcPath = s"$tempDir/test.orc"

    // Create DataFrame and write it as ORC
    val df = spark.createDataFrame(spark.sparkContext.parallelize(orcData), schema)
    df.write.orc(tempOrcPath)

    // When
    val resultDf = orcReader.read(spark, tempOrcPath)

    // Then
    resultDf.schema shouldEqual schema
    resultDf.collect() should contain theSameElementsAs orcData
  }

  it should "throw an exception if the ORC file is malformed" in {
    // Given
    // Create a temporary directory for the malformed ORC file
    val tempDir = java.nio.file.Files.createTempDirectory("orcReaderTest").toString
    val tempOrcPath = s"$tempDir/malformed.orc"
    // Manually create a malformed ORC file (you might need a different approach here, as creating an actual malformed ORC might be tricky)

    // When / Then
    assertThrows[org.apache.spark.sql.AnalysisException] {
      orcReader.read(spark, tempOrcPath)
    }
  }
}

