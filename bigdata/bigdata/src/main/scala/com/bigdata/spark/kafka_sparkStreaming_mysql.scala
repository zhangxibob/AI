package com.bigdata.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import java.sql.{Connection, DriverManager, Statement}

/**
 * @author 杨俊
 * @contact 咨询微信:dashuju_2017
 * @created time 2022-04-10
 */
object kafka_sparkStreaming_mysql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("advertise").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->"advertise",
      "auto.offset.reset"->"earliest",
      "enable.auto.commit"->(true:java.lang.Boolean)
    )

    val topics = Array("advertise")
    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )

    val lines = stream.map(record => record.value)

    //当前时间,省份编号,城市编号,用户id，广告id
    val filter = lines.map(_.split(",")).filter(_.length==5)
    //统计每个广告的点击量
    val adCounts = filter.map(x => (x(4),1)).reduceByKey(_+_)
    adCounts.foreachRDD(rdd=>{
      rdd.foreachPartition(myAdvertiseFun)
    })

    //统计每个省份广告点击量
    val provinceCounts = filter.map(x => (x(1),1)).reduceByKey(_+_)
    provinceCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(myProvinceFun)
    })

    //统计每个城市广告点击量
    val cityCounts = filter.map(x => (x(2),1)).reduceByKey(_+_)
    cityCounts.foreachRDD(rdd =>{
      rdd.foreachPartition(myCityFun)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def myAdvertiseFun(records:Iterator[(String,Int)]):Unit ={
    var conn = null
    var statement = null

    try{
     conn =  DriverManager.getConnection(Config.url,Config.userName,Config.passWord)
      records.foreach(t=>{
        val aid = t._1
        val count = t._2
        val sql = "select 1 from adversisecount "+" where aid = '"+aid+"'"
        val updateSql = "update adversisecount set count = count+"+count+" where aid = '"+aid+"'"
        val insertSql = "insert into adversisecount(aid,count) values('"+aid+"',"+count+")"

        statement = conn.createStatement()

        var resultSet = statement.executeQuery(sql)
        if(resultSet.next()) statement.executeUpdate(updateSql) else statement.execute(insertSql)
      })

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(statement!=null) statement.close()
      if(conn!=null) conn.close()
    }

  }

  def myProvinceFun(records:Iterator[(String,Int)]):Unit ={
    var conn = null
    var statement = null

    try{
      conn =  DriverManager.getConnection(Config.url,Config.userName,Config.passWord)
      records.foreach(t=>{
        val province = t._1
        val count = t._2
        val sql = "select 1 from provincecount "+" where province = '"+province+"'"
        val updateSql = "update provincecount set count = count+"+count+" where province = '"+province+"'"
        val insertSql = "insert into provincecount(province,count) values('"+province+"',"+count+")"

        statement = conn.createStatement()

        var resultSet = statement.executeQuery(sql)
        if(resultSet.next()) statement.executeUpdate(updateSql) else statement.execute(insertSql)
      })

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(statement!=null) statement.close()
      if(conn!=null) conn.close()
    }
  }

  def myCityFun(records:Iterator[(String,Int)]):Unit ={
    var conn = null
    var statement = null

    try{
      conn =  DriverManager.getConnection(Config.url,Config.userName,Config.passWord)
      records.foreach(t=>{
        val city = t._1
        val count = t._2
        val sql = "select 1 from citycount "+" where city = '"+city+"'"
        val updateSql = "update citycount set count = count+"+count+" where city = '"+city+"'"
        val insertSql = "insert into citycount(city,count) values('"+city+"',"+count+")"

        statement = conn.createStatement()

        var resultSet = statement.executeQuery(sql)
        if(resultSet.next()) statement.executeUpdate(updateSql) else statement.execute(insertSql)
      })

    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(statement!=null) statement.close()
      if(conn!=null) conn.close()
    }

  }

}
