package com.krishna.spark.scala.common

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import scala.util.{Try, Success, Failure}

/**
  * @author krishna on 2021-02-14
  */
object DatasetUtils {

  def readInputFile[T: Encoder](spark: SparkSession,
                                file_path: String,
                                file_type: String): Dataset[T] = {
    val ds = Try({
      val schema = implicitly[Encoder[T]].schema
      spark.read
        .format(file_type)
        .option("header", "true")
        .schema(schema)
        .csv(file_path)
        .as[T]
    })

    ds match {
      case Success(v) => v
      case Failure(problem) =>
        throw new Exception(
          f"dataset creation error ${file_path} -- ${problem.getMessage}")
    }

  }

}
