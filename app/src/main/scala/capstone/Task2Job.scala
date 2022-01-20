package capstone

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, sum}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}

class Task2Job(spark: SparkSession, conf: Config){

  def runTaskNum(taskNum: String): Unit = {
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(conf.getString("task11csv")).as[PurchAttrProj]
    taskNum match {
      case "task21" =>
        val res = topMarkCampGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet(conf.getString("task21"))
      case "task22" =>
        val res = topSesChanGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet(conf.getString("task21"))
      case "task21alt" =>
        val res = topMarkCampAltGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet(conf.getString("task21alt"))
      case "task22alt" =>
        val res = topSesChanAltGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet(conf.getString("task22alt"))
    }
  }

  private[capstone] def topMarkCampGen(task11out: Dataset[PurchAttrProj]):Dataset[TopMarkCamp] = {
    import spark.implicits._
    task11out.createOrReplaceTempView("task11out")
    val topCampQuery = """ select * from (select a.campaignId, a.billingSum, dense_rank() over (order by a.billingSum desc) as rank
                         | from (select campaignId, sum(billingCost) as billingSum
                         |from task11out
                         |where task11out.isConfirmed == True
                         |group by campaignId
                         |order by billingSum desc) a) b where b.rank<=10 """.stripMargin
    spark.sql(topCampQuery).as[TopMarkCamp]
  }
  private[capstone] def topSesChanGen(task11out: Dataset[PurchAttrProj]):Dataset[TopSesChan] = {
    import spark.implicits._
    task11out.createOrReplaceTempView("task11out")
    val chanEngPerfQuery = """select distinct tab1.* from (select campaignId, channelId, count(*) as sessionNum
                             |from task11out
                             |group by campaignId, channelId
                             |order by sessionNum desc) tab1
                             |left join (select campaignId, channelId, count(*) as sessionNum
                             |from task11out
                             |group by campaignId, channelId
                             |order by sessionNum desc) tab2
                             |on (tab1.campaignId=tab2.campaignId and tab1.sessionNum<tab2.sessionNum)
                             |where tab2.campaignId is null
                             |order by tab1.sessionNum
                             |""".stripMargin
    spark.sql(chanEngPerfQuery).as[TopSesChan]
  }
  private[capstone] def topSesChanAltGen(task11out: Dataset[PurchAttrProj]):Dataset[TopSesChan] = {
    import spark.implicits._
    task11out.select("campaignId", "channelId", "billingCost")
      .groupBy("campaignId", "channelId")
      .agg(
        count("billingCost").as("sessionNum"))
      .withColumn("sesmax", functions.max("sessionNum")
        .over(Window.partitionBy("campaignId")))
      .where(col("sessionNum") === col("sesmax"))
      .drop("sesmax").as[TopSesChan]
  }

  private[capstone] def topMarkCampAltGen(task11out: Dataset[PurchAttrProj]):Dataset[TopMarkCamp] = {
    import spark.implicits._
    task11out.select("campaignId", "billingCost")
      .groupBy("campaignId")
      .agg(sum("billingCost").as("billingSum"))
      .withColumn("rank", dense_rank().over(Window.orderBy(col("billingSum").desc)))
      .where(col("rank")<=10)
      .drop("billingCost")
      .orderBy(col("billingSum").desc)
      .as[TopMarkCamp]
  }
}

case class TopMarkCamp(campaignId: String,
                       billingSum: Double,
                       rank: Int)

case class TopSesChan(campaignId: String,
                      channelId: String,
                      sessionNum: BigInt)