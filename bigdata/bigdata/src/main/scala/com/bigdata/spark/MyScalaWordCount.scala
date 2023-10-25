package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 杨俊
 * @contact 咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
object MyScalaWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("Usage:MyScalaWordCount <input> <output>")
      System.exit(1)
    }

    val input = args(0)
    val output = args(1)

    val conf = new SparkConf().setAppName("MyScalaWordCount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(input)
    val resultRDD = lines.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)

    resultRDD.saveAsTextFile(output)

    sc.stop()

  }
}
