package com.krishna.spark.scala.batch

import com.krishna.spark.scala.common.{DatasetUtils, SparkApplicationContext}
import com.krishna.spark.scala.model.{Customer, Sale}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/**
  * @author krishna on 2021-02-14
  */
object CustSalesSkew extends App with SparkApplicationContext {

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
/*
  val agg_sales = sales_dates
    .join(cust, sales_dates("customer_id") === cust("customer_id"))
    .groupBy(cust("state"), sales_dates("year"))
    .agg(sum(sales_dates("sales_price")))
*/

  val custWithSkewKey =
    cust.withColumn("skew_key", explode(lit((0 to 9).toArray)))
  val salesWithSkewKey =
    sales_dates.withColumn("skew_key", monotonically_increasing_id() % 10)

  val agg_sales_skew = salesWithSkewKey
    .join(custWithSkewKey, Seq("customer_id", "skew_key"))
    .groupBy(custWithSkewKey("state"),
             salesWithSkewKey("hour"),
             salesWithSkewKey("day"),
             salesWithSkewKey("month"),
             salesWithSkewKey("year"))
    .agg(sum(salesWithSkewKey("sales_price")))

  agg_sales_skew.take(10)

  /* print("--- after adding skew column partition details ------------ ")
  agg_sales_skew
    .rdd
    .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
    .toDF("partition_number","number_of_records")
    .show
  print("partition details ------------ ")

  agg_sales_skew.explain()*/

  Thread.sleep(100000)
  spark.stop()

}
