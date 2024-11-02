package org.scala.spark.writer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JSONExporterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("JsonWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "JSONExporter" should "write DataFrame to JSON without partitioning and no option" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/json/no-partition"

    // Act
    JSONExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.json(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to JSON with partitioning and no option" in {
    // Arrange
    val data = Seq(("Alice", "2024-01-01", 1), ("Bob", "2024-01-01", 2), ("Charlie", "2024-01-02", 3))
    val df = spark.createDataFrame(data).toDF("name", "date", "id")
    val path = "test-output/json/with-partition"

    // Act
    JSONExporter.write(dataFrame = df, path = path, partitionBy = Seq("date"), saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.json(path + "/date=2024-01-01")
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to JSON with options" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/json/with-options"
    val options = Map("compression" -> "gzip")

    // Act
    JSONExporter.write(dataFrame = df, path = path, option = options, saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.option("compression", "gzip").json(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to JSON with default save mode" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/json/default-save-mode"

    // Act
    JSONExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.json(path)
    writtenDf.count() should be(2)
  }
}

