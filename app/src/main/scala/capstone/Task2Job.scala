package capstone

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, sum}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}

class Task2Job(spark: SparkSession){

  def runTaskNum(taskNum: String): Unit = {
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("output/task11outcsv").as[PurchAttrProj]
    taskNum match {
      case "task21" =>
        val res = topMarkCampGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet("output/task21out")
      case "task22" =>
        val res = topSesChanGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet("output/task22ut")
      case "task21alt" =>
        val res = topMarkCampAltGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet("output/task21altout")
      case "task22alt" =>
        val res = topSesChanAltGen(task11out)
        res.write.mode(SaveMode.Overwrite).parquet("output/task22altout")
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
/*
  def runT1(): Unit = {
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("output/task11out.csv").as[PurchAttrProj]
    val res = topMarkCampGen(task11out)
    res.write.mode(SaveMode.Overwrite).parquet("output/task21out")
  }

  def runT2(): Unit = {
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("output/task11out.csv").as[PurchAttrProj]
    val res = topSesChanGen(task11out)
    res.write.mode(SaveMode.Overwrite).parquet("output/task22out")
  }

  def runT1alt(): Unit ={
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("output/task11out.csv").as[PurchAttrProj]
    val res = topMarkCampAltGen(task11out)
    res.write.mode(SaveMode.Overwrite).parquet("output/task21altout")
  }

  def runT2alt(): Unit = {
    import spark.implicits._
    val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("output/task11out.csv").as[PurchAttrProj]
    val res = topSesChanAltGen(task11out)
    res.write.mode(SaveMode.Overwrite).parquet("output/task22altout")
  }
* */