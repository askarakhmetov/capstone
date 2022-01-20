package capstone

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class Task3Job(spark: SparkSession, conf: Config) {

  def runDateAndFormat(dateStart: String, dateEnd: String, format: String, version: String, givFun: Dataset[PurchAttrProj] => Dataset[_]): Unit = {
    import spark.implicits._
    format match {
      case "csv" =>
        val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
          .csv(conf.getString("task11csv")).as[PurchAttrProj]
        val ds = task11out.withColumn("date",
          to_date(col("purchaseTime"),"yyyy-MM-dd"))
          .filter(col("date") <= lit(dateEnd) and col("date") >= lit(dateStart))
        val res = givFun(ds.as[PurchAttrProj])

        reflect.io.File("output/query-"+version+"_"+dateStart+"_"+dateEnd+"_Csv.md").writeAll(task11out.queryExecution.toString())

        res.write.mode(SaveMode.Overwrite)
          .parquet(conf.getString("task3")+version+"_"+dateStart+"_"+dateEnd+"_"+format+"_"+"Out")
      case "parquet" =>
        val task11out = spark.read.options(Map("header" -> "true", "inferSchema" -> "true"))
          .csv(conf.getString("task11csv")).as[PurchAttrProj]
        task11out.withColumn("date",
          to_date(col("purchaseTime"),"yyyy-MM-dd"))
          .write
          .mode(SaveMode.Overwrite)
          .partitionBy("date")
          .parquet("output/inputTask3")
        val inputTask3parquet = spark.read.parquet("output/inputTask3")
          .filter(col("date") <= lit(dateEnd) and col("date") >= lit(dateStart))
          .as[PurchAttrProj]
        val res = givFun(inputTask3parquet)

        reflect.io.File("output/query_"+version+"_"+dateStart+"_"+dateEnd+"_Parquet.md").writeAll(inputTask3parquet.queryExecution.toString())

        res.write.mode(SaveMode.Overwrite)
          .parquet(conf.getString("task3")+version+"_"+dateStart+"_"+dateEnd+"_"+format+"_"+"Out")
    }
  }
  def runDateAndFormatV1(dateStart: String, dateEnd: String, format: String): Unit = {
    val t2J = new Task2Job(spark, conf: Config)
    runDateAndFormat(dateStart, dateEnd, format,"1", t2J.topMarkCampGen)
  }

  def runDateAndFormatV2(dateStart: String, dateEnd: String, format: String): Unit = {
    val t2J = new Task2Job(spark, conf: Config)
    runDateAndFormat(dateStart, dateEnd, format,"2", t2J.topSesChanGen)
  }
}
