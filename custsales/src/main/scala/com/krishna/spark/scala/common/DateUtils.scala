package com.krishna.spark.scala.common

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date}

import scala.util.Try

/**
  * @author krishna on 2021-02-14
  */
object DateUtils {
  def formatDateForEpoch(date: Long): LocalDate = {
    val dateFormat = "MM/dd/yyyy"
    val dtf = java.time.format.DateTimeFormatter.ofPattern(dateFormat)
    val dtTry = Try(new Date(date * 1000L))
    val dt = if (dtTry.isSuccess) dtTry.get else Calendar.getInstance.getTime
    val dtString = new SimpleDateFormat(dateFormat).format(dtTry.get)
    java.time.LocalDate.parse(dtString, dtf)
  }

}
