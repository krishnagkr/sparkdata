package com.krishna.spark.scala.batch

import com.krishna.spark.scala.common.{DatasetUtils, SparkApplicationContext}
import com.krishna.spark.scala.model.Customer
import com.krishna.spark.scala.model.Sale
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}

/**
  * @author krishna on 2021-02-14
  *
  * spark-submit --class com.krishna.spark.scala.batch.Application /Users/krishna/spark/spark-2.4.0-bin-hadoop2.7/bin/data/rest/custsales/target/uber-cust-sales-1.0-SNAPSHOT.jar
  *
  */
object CustSales extends App with SparkApplicationContext {

  import spark.implicits._
  val cust_file_path = args(0)
  val sales_file_path = args(1)

  val cust: Dataset[Customer] =
    DatasetUtils.readInputFile(spark, cust_file_path, "csv")
  val sales: Dataset[Sale] =
    DatasetUtils.readInputFile(spark, sales_file_path, "csv")

  val sales_dates = sales
    .withColumn("timestamp_date",
                from_unixtime($"timestamp", "dd-MM-yyyy HH:mm:ss"))
    .withColumn("date_time",
                to_timestamp(col("timestamp_date"), "dd-MM-yyyy HH:mm:ss"))
    .withColumn("hour", hour($"date_time"))
    .withColumn("day", dayofweek($"date_time"))
    .withColumn("month", month($"date_time"))
    .withColumn("year", year($"date_time"))

  import org.apache.spark.sql.functions._
  val agg_sales = sales_dates
    .join(cust, sales_dates("customer_id") === cust("customer_id"))
    .groupBy(cust("state"), sales_dates("year"))
    .agg(sum(sales_dates("sales_price")))

  agg_sales.take(10)

  /* // To Print query and partition details on console.
    agg_sales.explain()
    print("Partitions structure: {}".format(agg_sales.rdd.glom().collect()))
    sales_dates.unpersist()

    print("partition details ------------ ")
    agg_sales
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show
    print("partition details ------------ ")

   */

  Thread.sleep(100000)
  spark.stop()

}
