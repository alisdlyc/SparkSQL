package com.alisdlyc.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object LeetCodeSQLParse {
  def main(args: Array[String]): Unit = {
    val session = new SparkSession.Builder().master("local").appName("qwq").getOrCreate()
    val rdd = session.read.json("./src/com/alisdlyc/sparksql/ovo.json")
    rdd.printSchema()
    rdd.show()
    session.stop()
  }
}
