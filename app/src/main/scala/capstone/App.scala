/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package capstone

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

import annotation.tailrec
import scala.reflect.ClassTag
import java.sql.Timestamp

object App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_0.csv.gz")
    val df2 = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/user_purchases/user_purchases_0.csv.gz")

    df.createOrReplaceTempView("sdf")
    df2.createOrReplaceTempView("sdf2")

    val query2 = """SELECT sdf2.purchaseId, purchaseTime, billingCost, isConfirmed, tmps.userId as sessionId, campaignId, channelId
                   |    FROM sdf2
                   |    INNER JOIN (select userId, get_json_object(attributes, '$.purchase_id') as purchaseId
                              from sdf order by userId) as tmps
                   |    ON tmps.purchaseId = sdf2.purchaseId
                   |    INNER JOIN (select userId, get_json_object(attributes, '$.campaign_id') as campaignId,
                                            get_json_object(attributes, '$.channel_id') as channelId
                            from sdf
                            where get_json_object(attributes, '$.campaign_id') is not null
                            and get_json_object(attributes, '$.channel_id') is not null
                            order by userId) as tmpss
                   |    ON tmpss.userId = tmps.userId order by sdf2.purchaseId"""

    val query1 = """SELECT sdf2.purchaseId, purchaseTime, billingCost, isConfirmed, userId as sessionId, campaignId, channelId
            |    FROM sdf2 INNER JOIN (select userId,
                                      get_json_object(collect_list(attributes)[0], '$.campaign_id') as campaignId,
                                      get_json_object(collect_list(attributes)[0], '$.channel_id') as channelId,
                                      get_json_object(collect_list(attributes)[1], '$.purchase_id') as purchaseId
                                      from sdf group by userId) as tmps
              ON tmps.purchaseId = sdf2.purchaseId order by sdf2.purchaseId"""

    val projTable1 = spark.sql(query1.stripMargin)
    val projTable2 = spark.sql(query2.stripMargin)

    projTable1.show(5, truncate = false)
    projTable2.show(5, truncate = false)

    import spark.implicits._
    val dfds: Dataset[MobAppClickProj] =
      df.withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType)))
        .withColumn("eventTime", unix_timestamp(col("eventTime"), "yyyy-MM-dd HH:mm:ss")
          .cast("timestamp"))
        .as[MobAppClickProj]
    dfds.show(5,truncate = false)

    val extractingPurch: TypedColumn[MobAppClickProj, Set[String]] =
      new Aggregator[MobAppClickProj, Set[String], Set[String]] with Serializable {
        def zero: Set[String] = Set[String]()
        def reduce(es: Set[String], macp: MobAppClickProj): Set[String] =
          if (macp.attributes.getOrElse(None)!=None) {
            val ttt = macp.attributes.get
            if (ttt.contains("purchase_id")) {es+ttt("purchase_id")}
            else es
          } else es
        def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
        def finish(columning: Set[String]): Set[String] = columning
        def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
        def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
      }.toColumn
    val extractingChan: TypedColumn[MobAppClickProj, Set[String]] =
      new Aggregator[MobAppClickProj, Set[String], Set[String]] with Serializable {
        def zero: Set[String] = Set[String]()
        def reduce(es: Set[String], macp: MobAppClickProj): Set[String] =
          if (macp.attributes.getOrElse(None)!=None) {
            val ttt = macp.attributes.get
            if (ttt.contains("channel_id")) {es+ttt("channel_id")}
            else es
          } else es
        def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
        def finish(columning: Set[String]): Set[String] = columning
        def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
        def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
      }.toColumn
    val extractingCamp: TypedColumn[MobAppClickProj, Set[String]] =
      new Aggregator[MobAppClickProj, Set[String], Set[String]] with Serializable {
        def zero: Set[String] = Set[String]()
        def reduce(es: Set[String], macp: MobAppClickProj): Set[String] =
          if (macp.attributes.getOrElse(None)!=None) {
            val ttt = macp.attributes.get
            if (ttt.contains("campaign_id")) {es+ttt("campaign_id")}
            else es
          } else es
        def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
        def finish(columning: Set[String]): Set[String] = columning
        def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
        def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
      }.toColumn

    dfds.groupByKey(_.userId)
      .agg(
        // our aggregator functions at work
        extractingPurch.name("purchaseId"),
        extractingCamp.name("campaignId"),
        extractingChan.name("channelId")
      ).show(5)

  }

}

case class MobAppClickProj(userId: String,
                           eventId: String,
                           eventTime: Timestamp,
                           eventType: String,
                           attributes: Option[Map[String, String]])

case class PurchasesProj(purchaseId: String,
                         purchaseTime: Timestamp,
                         billingCost: Double,
                         isConfirmed: Boolean)

case class PurchAttrProj(purchaseId: String,
                         purchaseTime: Timestamp,
                         billingCost: Double,
                         isConfirmed: Boolean,
                         // a session starts with app_open event and finishes with app_close
                         sessionId: String,
                         campaignId: String, // derived from app_open#attributes#campaign_id
                         channelId: String // derived from app_open#attributes#channel_id
                        )