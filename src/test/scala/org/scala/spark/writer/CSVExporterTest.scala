package org.scala.spark.writer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CSVExporterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("CsvWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "CSVExporter" should "write DataFrame to CSV without partitioning and no option" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/no-partition"

    // Act
    CSVExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.csv(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to CSV with partitioning and no option" in {
    // Arrange
    val data = Seq(("Alice", "2024-01-01", 1), ("Bob", "2024-01-01", 2), ("Charlie", "2024-01-02", 3))
    val df = spark.createDataFrame(data).toDF("name", "date", "id")
    val path = "test-output/with-partition"

    // Act
    CSVExporter.write(dataFrame = df, path = path, partitionBy = Seq("date"), saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.csv(path + "/date=2024-01-01")
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to CSV with options and without partition" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/with-options"
    val options = Map("delimiter" -> ";", "quote" -> "\"", "header" -> "true")

    // Act
    CSVExporter.write(dataFrame = df, path = path, option = options, saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.option("header", "true").option("delimiter", ";").csv(path)
    writtenDf.count() should be(2)
    writtenDf.columns should contain allOf("name", "id")
  }

  it should "write DataFrame to CSV with default save mode without header and partition" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/default-save-mode"

    // Act
    CSVExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.csv(path)
    writtenDf.count() should be(2)
  }
}



