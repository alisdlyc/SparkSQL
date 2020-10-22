package com.alisdlyc.sparksql

import org.apache.spark.sql.SparkSession

object DataFrameGo {
  def main(args: Array[String]): Unit = {
    val session = new SparkSession.Builder().master("local").appName("qwq").getOrCreate()
    val df = session.read.json("./src/com/alisdlyc/sparksql/qwq")

    df.printSchema()
    df.show()
    // select name from table
    df.select("name").show()
    /**
     * select name, age+10 as ages
     * from table
     *
     * null + 10 = null
     * */
    df.select(df.col("name"), (df.col("age")+10).as("ages")).show()

    /**
     * select *
     * from table
     * where age > 2
     * */
    df.filter(df.col("age") > 2).show()

    /**
     * select age, count(1)
     * from table
     * group by age(Having)
     * */
    df.groupBy("age").count().show()
    // df.where()


    session.stop()
  }

}
