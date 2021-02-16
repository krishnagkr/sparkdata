package com.krishna.spark.scala.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author krishna on 2021-02-14
  */
trait SparkApplicationContext {

  lazy val sparkConf = new SparkConf()
    .setAppName("SparkApplication-CustSales")
    .setMaster("local[*]")
    .set("spark.cores.max", "4")
    //.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.shuffle.partitions", "6")
    .set("spark.default.parallelism", "6")
   // .set("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")

  lazy val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

}
