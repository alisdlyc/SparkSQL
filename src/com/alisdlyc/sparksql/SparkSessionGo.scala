package com.alisdlyc.sparksql

import org.apache.spark.sql.SparkSession

object SparkSessionGo {

  def main(args: Array[String]): Unit = {
    val session = new SparkSession.Builder().appName("qwq").getOrCreate()
    val people_json = session.read.json()
    val qwq = session.read.format("json")
  }
}
