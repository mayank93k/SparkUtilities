package org.scala.spark.writer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class parquetWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ParquetWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "parquetWriter" should "write DataFrame to Parquet without partitioning and no options" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/parquet/no-partition"

    // Act
    parquetWriter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.parquet(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to Parquet with partitioning and no options" in {
    // Arrange
    val data = Seq(("Alice", "2024-01-01", 1), ("Bob", "2024-01-01", 2), ("Charlie", "2024-01-02", 3))
    val df = spark.createDataFrame(data).toDF("name", "date", "id")
    val path = "test-output/parquet/with-partition"

    // Act
    parquetWriter.write(dataFrame = df, path = path, partitionBy = Seq("date"), saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.parquet(path + "/date=2024-01-01")
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to Parquet with specified options" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/parquet/with-options"
    val options = Map("compression" -> "gzip")

    // Act
    parquetWriter.write(dataFrame = df, path = path, option = options, saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.parquet(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to Parquet with default save mode" in {
    // Arrange
    val data = Seq(("Alice", 1), ("Bob", 2))
    val df = spark.createDataFrame(data).toDF("name", "id")
    val path = "test-output/parquet/default-save-mode"

    // Act
    parquetWriter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.parquet(path)
    writtenDf.count() should be(2)
  }
}

