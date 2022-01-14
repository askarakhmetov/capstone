package capstone

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Task1JobTest extends AnyFunSuite {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()
  import spark.implicits._
  val startMac: DataFrame = Seq(("f6e8252f","3fce7a72","app_open","2020-12-13 15:37:31","{'campaign_id': 332, 'channel_id': 'Facebook Ads'}"),
    ("f6e8252f","b49802f8","search_product","2020-12-13 15:45:34",""),
    ("f6e8252f","bb6a","purchase","2020-11-01 16:56:07","{'purchase_id': 'e9bb17bc'}"),
  ("29d89ee4fa9e","d62d-4b21","view_product_","2020-12-13 15:48:07","{'campaign_id': 235, 'channel_id': 'Google Ads'}"),
  ("29d89ee4fa9e","47cb","app_close","2020-12-13 16:04:20","{'purchase_id': '71545a13'}"))
    .toDF("userId","eventId","eventType","eventTime","attributes")
  val startUp: DataFrame = Seq(("5a921187","2020-12-16 07:23:14","310.28","False"),
  ("e9bb17bc","2020-11-01 16:56:07","481.83","True"),
  ("71545a13","2020-11-12 13:58:55","587.55","False"),
  ("8c778ea6","2020-09-25 17:26:20","909.03","True"))
    .toDF("purchaseId","purchaseTime","billingCost","isConfirmed")

  val expected: DataFrame = Seq(("e9bb17bc", "2020-11-01 16:56:07", "481.83", "True", "f6e8252f", "332", "Facebook Ads"),
    ("71545a13", "2020-11-12 13:58:55", "587.55", "False", "29d89ee4fa9e", "235", "Google Ads"))
    .toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelId")
    .withColumn("purchaseTime",
      to_timestamp(col("purchaseTime")))
    .withColumn("billingCost",
      col("billingCost").cast(DoubleType))
    .withColumn("isConfirmed",
      col("isConfirmed").cast("boolean"))
  //(purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean, sessionId: String, campaignId: String, channelId: String)

  val taskJob = new Task1Job(spark)

  test("testing runV1") {
    val res = taskJob.purchAttrProjGen(startUp, startMac)
    val diff = res.toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelId")
      .except(expected)
      .union(expected.except(res.toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelId")))
    assert(diff.count()==0)
  }
  test("testing runV2") {
    val res = taskJob.purchAttrProjAggGen(startUp, startMac)
    val diff = res.toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelId")
      .except(expected)
      .union(expected.except(res.toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed", "sessionId", "campaignId", "channelId")))
    assert(diff.count()==0)
  }
}