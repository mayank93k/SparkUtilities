package org.scala.spark.reader

import org.apache.spark.sql.SparkSession

object bqReaderTest {
  def main(args: Array[String]): Unit = {
    try {
      // Dummy BigQuery read setup
      val sparkSession = SparkSession.builder().appName("DummyTest").master("local").getOrCreate()
      val project = "my-project"
      val dataset = "my-dataset"
      val table = "my-project:my-dataset.my-table" // Ensure this is correctly formatted

      // Attempt to read from BigQuery
      val df = bqReader.read(sparkSession, project, dataset, table)

      // Display DataFrame schema for testing
      df.printSchema()
    } catch {
      case e: IllegalArgumentException =>
        println(s"Handled exception: ${e.getMessage}")
      case e: Exception =>
        println(s"Unhandled exception: ${e.getMessage}")
    }
  }
}

