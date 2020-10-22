package com.alisdlyc.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DateFrame App").master("local").getOrCreate()
    //    PersonReflect(spark)
    val rdd = spark.sparkContext.textFile("./src/com/alisdlyc/sparksql/person.txt")
    val PersonRDD = rdd.map(_.split(","))
      .map(line => Person(line(0).toInt, line(1), line(2).toInt))
    val structType =StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    spark.stop()
  }

  private def PersonReflect(spark: SparkSession) = {
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("./src/com/alisdlyc/sparksql/person.txt")

    // 需要导入隐式类型转换，不然无法调用 RDD.toDF()
    import spark.implicits._
    val PersonDF = rdd.map(_.split(",")).map(line => Person(line(0).toInt, line(1), line(2).toInt)).toDF()
    PersonDF.show()
    PersonDF.filter(PersonDF.col("age") > 19).show()
  }

  case class Person(id: Int, name: String, age: Int)

}
