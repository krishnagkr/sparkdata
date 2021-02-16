package com.krishna.spark.scala.model
import java.sql.Timestamp

/**
  * @author krishna on 2021-02-14
  */
final case class Sale(
    timestamp: String,
    customer_id: Int,
    sales_price: Double,
    shipping_address: String
)
