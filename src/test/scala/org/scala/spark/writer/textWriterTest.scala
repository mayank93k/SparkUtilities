package org.scala.spark.writer

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class textWriterTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("TextWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "textWriter" should "write DataFrame to text format without partitioning and no options" in {
    // Arrange
    val data = Seq("Alice", "Bob")
    val df = spark.createDataFrame(data.map(Tuple1(_))).toDF("value")
    val path = "test-output/text/no-partition"

    // Act
    textWriter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.text(path)
    writtenDf.count() should be(2)
    writtenDf.collect().map(_.getString(0)) should contain allOf("Alice", "Bob")
  }

  it should "write DataFrame to text format with partitioning and no options" in {
    // Arrange
    val data = Seq(("Alice", "2024-01-01"), ("Bob", "2024-01-01"), ("Charlie", "2024-01-02"))
    val df = spark.createDataFrame(data).toDF("value", "date")
    val path = "test-output/text/with-partition"

    // Act
    textWriter.write(dataFrame = df, path = path, partitionBy = Seq("date"), saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.text(path + "/date=2024-01-01")
    writtenDf.count() should be(2)
    writtenDf.collect().map(_.getString(0)) should contain allOf("Alice", "Bob")
  }

  it should "write DataFrame to text format with specified options" in {
    // Arrange
    val data = Seq("Alice", "Bob")
    val df = spark.createDataFrame(data.map(Tuple1(_))).toDF("value")
    val path = "test-output/text/with-options"
    val options = Map("compression" -> "gzip")

    // Act
    textWriter.write(dataFrame = df, path = path, option = options, saveMode = SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.text(path)
    writtenDf.count() should be(2)
  }

  it should "write DataFrame to text format with default save mode" in {
    // Arrange
    val data = Seq("Alice", "Bob")
    val df = spark.createDataFrame(data.map(Tuple1(_))).toDF("value")
    val path = "test-output/text/default-save-mode"

    // Act
    textWriter.write(df, path, SaveMode.Overwrite)

    // Assert
    val writtenDf = spark.read.text(path)
    writtenDf.count() should be(2)
    writtenDf.collect().map(_.getString(0)) should contain allOf("Alice", "Bob")
  }
}

