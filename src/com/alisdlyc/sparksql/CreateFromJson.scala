package com.alisdlyc.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateFromJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Create from Json")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    /**
     * DataFrame 有数据 有scheme信息
     * sqlContext 读取json记录生成DataFrame，DataFrame中的列会按照ASSIC码进行排序（小的在前面）
     *
     * DataFrame 不能读取嵌套格式的json，会丢失列的属性信息
     * */
    val df = sqlContext.read.format("json").load("./src/com/alisdlyc/sparksql/qwq")
    df.show()
    df.printSchema()

    /**
     * 将DataFrame注册成为临时表,然后就可以随便玩耍了
     * */
    df.registerTempTable("qwq")
    val re0 = sqlContext.sql("select * from qwq")
    val re1 = sqlContext.sql("select * from qwq where age > 1")
    re0.show()
    re1.show()

    val re2 = sqlContext.sql("SELECT name, age from qwq")
    re2.show()

    sc.stop()
  }
}
