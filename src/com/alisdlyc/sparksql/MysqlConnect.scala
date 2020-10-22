package com.alisdlyc.sparksql

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


object MysqlConnect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("mysql")

    /**
     * 配置join或者聚合操作shuffle数据时分区的数量
     *  分区数量太多在存储量较小时，可能会花费很多的时间在启动和停止task上面，在数据量较小时，可以适当的减少分区数目，提高效率
     * */
    conf.set("spark.sql.shuffle.partitions", "1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val reader = sqlContext.read.format("jdbc")
    reader.option("url", "jdbc:mysql://localhost:3306/alisdlyc")
    reader.option("driver", "com.mysql.jdbc.Driver")
    reader.option("useSSL", "false")
    reader.option("user", "root")
    reader.option("password", "Sd5.35365")
    reader.option("datable", "Orders")

    val df = reader.load()
    df.show()
    df.printSchema()
    df.registerTempTable("Orders")


    /**
     * 将DataFrame中的结果写入数据库中
     *
     * */
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "Sd5.35365")
    properties.setProperty("useSSL", "false")
    /**
     * SaveMode:
     *  OverWrite: 覆盖
     *  Append： 追加
     *  ErrorIfExist： 如果存在就报错
     *  Ignore： 如果存在就忽略
     * */
    df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/alisdlyc", "result", properties)
    println("-----成功存入数据库中------")
    sc.stop()
  }
}
