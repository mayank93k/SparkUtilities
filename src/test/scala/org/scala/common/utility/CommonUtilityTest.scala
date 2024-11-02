package org.scala.common.utility

import org.scala.common.utility.CommonUtility.dateTimeInMilliConvertor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class CommonUtilityTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  "dateTimeInMilliConvertor" should "return invalid date format" in {
    val dateStr = "20231102" // Example integer date string
    val timestamp: Option[Timestamp] = dateTimeInMilliConvertor(dateStr)

    timestamp match {
      case Some(ts) => println(s"Converted Timestamp: $ts")
      case None => println("Invalid date format")
    }
  }

  "dateTimeInMilliConvertor" should "convert date in milli convertor" in {
    val dateStr = "2023-11-02.15.30.45.123" // Example date string
    val timestamp: Option[Timestamp] = dateTimeInMilliConvertor(dateStr)

    timestamp match {
      case Some(ts) => println(s"Converted Timestamp: $ts")
      case None => println("Invalid date format")
    }
  }


}
