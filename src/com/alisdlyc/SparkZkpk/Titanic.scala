package com.alisdlyc.SparkZkpk

import org.apache.spark.{SparkConf, SparkContext}

object Titanic {
  class SecondSortKey(val first: Double, val second: Double, val third: Double) extends Ordered[SecondSortKey] with Serializable {
    override def compare(that: SecondSortKey): Int = {
      if (this.first -that.first != 0) {
        (this.first - that.first).toInt
      } else if(this.second - that.second != 0) {
        (that.second - this.second).toInt
      } else {
        (this.third - that.third).toInt
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Titanic").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("/home/zkpk/IdeaProjects/spark_test/src/main/scala/org/zkpk/lab/titanic.csv")
    val rdd1 = rdd.map(_.split(","))
    // 3G 10M 6G
    //		val rdd2 = rdd1.map { x => ((x(3), x(10), x(6)), (x(4), x(2))) }

    val rdd2 = rdd1.zipWithIndex().filter(_._2 >= 1).keys
    val rdd3 = rdd2.map { x => ((x(2), x(9), x(5)), (x(3), x(1))) }
    val re = rdd3.map { x => (new SecondSortKey(x._1._1.toDouble, x._1._2.toDouble, x._1._3.toDouble), x)}.sortByKey().map(_._2)
    re.saveAsTextFile("hdfs://master:9000/sparktest/titanic/debug1")
    sc.stop()
  }
}
