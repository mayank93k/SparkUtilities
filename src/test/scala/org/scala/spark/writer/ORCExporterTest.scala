package org.scala.spark.writer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ORCExporterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("OrcWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "ORCExporter" should "write DataFrame to ORC without partitioning and no options" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/orc/no-partition"

    // Act
    ORCExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.orc(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to ORC with partitioning and no options" in {
    // Arrange
    val data = Seq(("Alice", "2024-01-01", 1), ("Bob", "2024-01-01", 2), ("Charlie", "2024-01-02", 3))
    val df = spark.createDataFrame(data).toDF("name", "date", "id")
    val path = "test-output/orc/with-partition"

    // Act
    ORCExporter.write(dataFrame = df, path = path, partitionBy = Seq("date"), saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.orc(path + "/date=2024-01-01")
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to ORC with specified options" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/orc/with-options"
    val options = Map("compression" -> "snappy")

    // Act
    ORCExporter.write(dataFrame = df, path = path, options = options, saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.orc(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to ORC with default save mode" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/orc/default-save-mode"

    // Act
    ORCExporter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.orc(path)
    writtenDf.count() should be(2)
  }
}

