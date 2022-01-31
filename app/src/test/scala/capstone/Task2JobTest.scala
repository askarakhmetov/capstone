package capstone

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.Timestamp

@RunWith(classOf[JUnitRunner])
class Task2JobTest extends AnyFunSuite {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()
  import spark.implicits._
  val taskJob = new Task2Job(spark, ConfigFactory.load())
  val inData: Dataset[PurchAttrProj] = spark.createDataset(
    Seq(PurchAttrProj("e9bb17bc", Timestamp.valueOf("2020-11-01 16:56:07"), 481.8, isConfirmed = true, "f6e8252f", "332", "Facebook Ads"),
      PurchAttrProj("sdzfbc", Timestamp.valueOf("2020-11-01 16:56:07"), 0.0, isConfirmed = true, "f6e8252f", "332", "Facebook Ads"),
      PurchAttrProj("e9bb17bc", Timestamp.valueOf("2020-11-01 16:56:07"), 234.6, isConfirmed = true, "f6e8sdf", "234", "Facebook Ads"),
      PurchAttrProj("nere7gtr", Timestamp.valueOf("2020-11-01 16:56:07"), 200.0, isConfirmed = true, "458252f", "332", "Google Ads"),
      PurchAttrProj("h7e78wh", Timestamp.valueOf("2020-11-01 16:56:07"), 90.4, isConfirmed = true, "t6e3454252f", "456", "VK Ads")))

  //(purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean, sessionId: String, campaignId: String, channelId: String)

  val expectedT1: DataFrame = Seq(("332",681.8,1),("234",234.6,2),("456",90.4,3))
    .toDF("campaignId", "billingSum", "rank")
  //(campaignId: String, billingSum: Double, rank: Int)
  val expectedT2: DataFrame = Seq(("456","VK Ads",1),("332","Facebook Ads",2),("234","Facebook Ads",1))
    .toDF("campaignId", "channelId", "sessionNum")
  //(campaignId: String, channelId: String, sessionNum: BigInt)
  test("testing runT1") {
    val res = taskJob.topMarkCampGen(inData)
    val diff = res.toDF("campaignId", "billingSum", "rank").except(expectedT1)
      .union(expectedT1.except(res.toDF("campaignId", "billingSum", "rank")))
    assert(diff.count()==0)
  }
  test("testing runT2") {
    val res = taskJob.topSesChanGen(inData)
    val diff = res.toDF("campaignId", "channelId", "sessionNum").except(expectedT2)
      .union(expectedT2.except(res.toDF("campaignId", "channelId", "sessionNum")))
    assert(diff.count()==0)
  }
  test("testing runT1alt") {
    val res = taskJob.topMarkCampAltGen(inData)
    println("res")
    res.show()
    println("expected")
    expectedT1.show()
    val diff = res.toDF("campaignId", "billingSum", "rank").except(expectedT1)
      .union(expectedT1.except(res.toDF("campaignId", "billingSum", "rank")))
    assert(diff.count()==0)
  }
  test("testing runT2alt") {
    val res = taskJob.topSesChanAltGen(inData)
    val diff = res.toDF("campaignId", "channelId", "sessionNum").except(expectedT2)
      .union(expectedT2.except(res.toDF("campaignId", "channelId", "sessionNum")))
    assert(diff.count()==0)
  }
}
