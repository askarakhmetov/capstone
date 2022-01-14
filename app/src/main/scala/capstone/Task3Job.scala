package capstone

import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.io.{File, PrintWriter}

class Task3Job(spark: SparkSession, task11out: Dataset[PurchAttrProj]) {

  private val mac_csv = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/mobile_app_clickstream/mobile_app_clickstream_*.csv.gz")
  private val up_csv = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("capstone-dataset/user_purchases/user_purchases_*.csv.gz")

  mac_csv.write.mode(SaveMode.Overwrite).parquet("output/mac")
  up_csv.write.mode(SaveMode.Overwrite).parquet("output/up")

  private val mac_parq = spark.read.parquet("output/mac")
  private val up_parq = spark.read.parquet("output/up")

  import spark.implicits._

  task11out.withColumn("date",
    to_date(col("purchaseTime"),"yyyy-MM-dd"))
    .write.option("header", "true").csv("output/inputTask3.csv")

  private val inputTask3csv = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
    .csv("output/inputTask3.csv")

  task11out.withColumn("date",
    to_date(col("purchaseTime"),"yyyy-MM-dd"))
    .write
    .partitionBy("date")
    .parquet("output/inputTask3")

  private val inputTask3parquet = spark.read.parquet("output/inputTask3")

  private val t2J = new Task2Job(spark)
  private val task321SeptCsv = inputTask3csv.filter(col("date").lt(lit("2020-10-01")).gt(lit("2020-08-11")))
  private val task321SeptParquet = inputTask3parquet.filter(col("date").lt(lit("2020-10-01")).gt(lit("2020-08-11")))
  private val task321NovCsv = inputTask3csv.filter(col("date")===lit("2020-11-11"))
  private val task321NovParquet = inputTask3parquet.filter(col("date")===lit("2020-11-11"))

  private val writer = new PrintWriter(new File("output/queries.md" ))
  writer.write("parquet \n --------------------------- \n"+inputTask3parquet.queryExecution.toString()+"\n")
  writer.write("csv \n --------------------------- \n"+inputTask3csv.queryExecution.toString())
  writer.close()

  def runV1SeptCsv(): Unit = {
    val res = t2J.topMarkCampGen(task321SeptCsv.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task31SeptCsvOut")
  }

  def runV1SeptParquet(): Unit = {
    val res = t2J.topMarkCampGen(task321SeptParquet.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task31SeptParquetOut")
  }

  def runV1NovCsv(): Unit = {
    val res = t2J.topMarkCampGen(task321NovCsv.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task31NovCsvOut")
  }

  def runV1NovParquet(): Unit = {
    val res = t2J.topMarkCampGen(task321NovParquet.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task31NovParquetOut")
  }

  def runV2SeptCsv(): Unit = {
    val res = t2J.topSesChanGen(task321SeptCsv.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task32SeptCsvOut")
  }

  def runV2SeptParquet(): Unit = {
    val res = t2J.topSesChanGen(task321SeptParquet.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task32SeptParquetOut")
  }

  def runV2NovCsv(): Unit = {
    val res = t2J.topSesChanGen(task321NovCsv.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task32NovCsvOut")
  }

  def runV2NovParquet(): Unit = {
    val res = t2J.topSesChanGen(task321NovParquet.as[PurchAttrProj])
    res.write.mode(SaveMode.Overwrite).parquet("output/task32NovParquetOut")
  }

}
