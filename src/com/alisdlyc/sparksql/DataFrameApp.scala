package com.alisdlyc.sparksql

import org.apache.spark.sql.SparkSession

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[2]").appName("dataframe app").getOrCreate()

    val df = spark.read.json("/Users/alisdlyc/IdeaProjects/SparkSQL/src/com/alisdlyc/sparksql/ovo.json")
    df.printSchema()
    df.show()
    spark.stop()
  }
}
