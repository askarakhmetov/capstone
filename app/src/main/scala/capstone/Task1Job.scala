package capstone

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, MapType, StringType}
import org.apache.spark.sql._

class Task1Job(spark: SparkSession){

  def runT1(): Dataset[PurchAttrProj] ={
    val mac = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_*.csv.gz")
    val up = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/user_purchases/user_purchases_*.csv.gz")
    val res = purchAttrProjGen(up, mac)
    res.write.mode(SaveMode.Overwrite).parquet("output/task11out")
    res
  }

  def runT2(): Dataset[PurchAttrProj] ={
    val mac = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_*.csv.gz")
    val up = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/user_purchases/user_purchases_*.csv.gz")

    val res = purchAttrProjAggGen(up, mac)
    res.write.mode(SaveMode.Overwrite).parquet("output/task12out")
    res
  }


  private[capstone] def purchAttrProjGen(up: DataFrame, mac: DataFrame): Dataset[PurchAttrProj]={

    import spark.implicits._
    mac.createOrReplaceTempView("sdf")
    up.createOrReplaceTempView("sdf2")

    val query = """SELECT sdf2.purchaseId, purchaseTime, billingCost, isConfirmed, tmps.userId as sessionId, campaignId, channelId
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
                  |    ON tmpss.userId = tmps.userId order by sdf2.purchaseId""".stripMargin
    spark.sql(query).withColumn("purchaseTime",
      to_timestamp(col("purchaseTime")))
      .withColumn("billingCost",
        col("billingCost").cast(DoubleType))
      .withColumn("isConfirmed",
        col("isConfirmed").cast("boolean")).as[PurchAttrProj]
  }

  private[capstone] def purchAttrProjAggGen(up: DataFrame, mac: DataFrame): Dataset[PurchAttrProj] = {
    import spark.implicits._
    up.join(mac
            .withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType)))
            .withColumn("eventTime", to_timestamp(col("eventTime")))
            .as[MobAppClickProj]
            .groupByKey(_.userId)
            .agg(aggDfa.name("aggcol"))
            .withColumn("purchaseId", col("aggcol")(2))
            .withColumn("campaignId", col("aggcol")(0))
            .withColumn("channelId", col("aggcol")(1))
            //.withColumnRenamed("value", "sessionId")
            .drop("aggcol"), Seq("purchaseId"))
      .withColumn("purchaseTime",
        to_timestamp(col("purchaseTime")))
      .withColumnRenamed("key", "sessionId")
      .withColumn("billingCost",
        col("billingCost").cast(DoubleType))
      .withColumn("isConfirmed",
        col("isConfirmed").cast("boolean"))
      .as[PurchAttrProj]
  }

  val aggDfa : TypedColumn[MobAppClickProj, Set[String]] =
    new Aggregator[MobAppClickProj, Set[String], Set[String]] with Serializable {
      def zero: Set[String] = Set[String]()
      def reduce(es: Set[String], macp: MobAppClickProj): Set[String] = {
        macp.attributes match {
          case Some(value) => if (value.contains("campaign_id")) {es+value("campaign_id")+value("channel_id")}
                              else if (value.contains("purchase_id")) {es+value("purchase_id")} else es
          case None => es
        }
      }

      def merge(wx: Set[String], wy: Set[String]): Set[String] = wx.union(wy)
      def finish(columning: Set[String]): Set[String] = columning
      def bufferEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
      def outputEncoder: Encoder[Set[String]] = implicitly(ExpressionEncoder[Set[String]])
    }.toColumn
}