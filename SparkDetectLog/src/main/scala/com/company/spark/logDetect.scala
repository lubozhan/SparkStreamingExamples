package com.company.spark

import java.text.SimpleDateFormat

import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.HttpPost
import org.apache.http.HttpHeaders
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.log4j.Logger
import scala.collection.mutable

/**
  * Created by Roy on 2018/5/9.
  */

object logDetect {
  val logger = Logger.getLogger(logDetect.getClass)

  //record event from kafka
  case class Event(interfaceName:String, time:String, ip:String)
  //add count to the event
  case class newEvent(interfaceName:String, time:String, ip:String, count:Long)

  private val keyWords = "interface_1, interface_2, interface_3"
  private val keyMap = Map("interface_1" -> "查询客户信息",
    "interface_2" -> "查询电话信息",
    "interface_3" -> "查询余额信息"
  )

  //parse parameters from program arguments
  def processingParameters(args: Array[String]): mutable.HashMap[String, String] ={
    val argMap=new mutable.HashMap[String,String]()
    args.map(x=>{
      val props =StringUtils.substringsBetween(x,"[","]")
      argMap.put(props(0),props(1))
    })
    argMap
  }

  //transfer string date to date
  def parseDate(value: String) = {
    try {
      Some(new SimpleDateFormat("yyyyMMddHHmmss").parse(value))
    } catch {
      case  e: Exception => None
    }
  }

  // Post json to http server for future process
  def Process(record:newEvent, keymap: Map[String, String]) :Unit = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val currentTime = df.format(System.currentTimeMillis())
    val body: String =
      f"""{
         |        "uid": "110",
         |        "name": "Roy#",
         |        "password": "123"
         |}""".stripMargin

    val url = "http://localhost:8088"
    val httpClient = HttpClients.createDefault()
    val post = new HttpPost(url)
    post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
    post.setEntity(new StringEntity(body))
    println(post.getEntity.getContent)

    try {
      val response = httpClient.execute(post)
      println("--- HEADERS ---")
      response.getAllHeaders.foreach(arg => println(arg))

      println(response.getStatusLine.getStatusCode)
      println(response.getEntity.getContent)

    } catch {
      case e:Exception => {
      logger.error(e.printStackTrace())
      }
    } finally {
      httpClient.close()
    }
  }

  /**
    * Detect specified Ip which access specified interface in windowslides more times than threshold
    *       program arguments like:
    *       [kafka_broker][localhost:9092]
    *       [kafka_topic][topic]
    *       [kafka_groupid][test]
    *       [durations][10]
    *       [window_duration][30]
    *       [slide_duration][10]
    *       [threshold][2]
    *       [offsetReset][largest]
   */

  def main(args: Array[String]): Unit = {

    val params=processingParameters(args)
    val Array(brokerList, topic, gropuId, batchSize, windowDuration, slideDuration, threshold, offsetReset) =
      Array(params.get("kafka_broker"),params.get("kafka_topic"),params.get("kafka_groupid"),params.get("durations"),params.get("window_duration"),params.get("slide_duration"),params.get("threshold"), params.get("offsetReset"))

    val conf = new SparkConf().setAppName("SparkDetectLog").setMaster("local")
    val ssc=new StreamingContext(conf,Seconds(batchSize.get.toLong))

    val keyword = keyWords.split(",").toSet
    val b_keyword = ssc.sparkContext.broadcast(keyword)
    val b_windowDuration = ssc.sparkContext.broadcast(windowDuration.get)
    val b_slideDuration = ssc.sparkContext.broadcast(slideDuration.get)
    val b_threshold = ssc.sparkContext.broadcast(threshold.get)
    val b_keyMap = ssc.sparkContext.broadcast(keyMap)

    val aggregateFunc = (v1: newEvent, v2 :newEvent) => {
      val date_1 = parseDate(v1.time)
      val date_2 = parseDate(v2.time)
      val date = if(date_1.get.before(date_2.get)) v2.time else v1.time
      val event = new newEvent(v1.interfaceName, date, v1.ip,(v1.count + v2.count))
      event
    }

    val filterFunc = (p:newEvent) =>
      if (p.count > b_threshold.value.toLong) true else false

    val topicSet: Set[String] =topic.get.toString.split(",").toSet
    val kafkaParams=Map("metadata.broker.list"->brokerList.get, "auto.offset.reset" -> offsetReset.get, "group.id"->gropuId.get)
    val InputStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)


    InputStream.flatMap(line =>{
      implicit val formats = DefaultFormats
      val data = parse(line._2)
      val event = data.extract[Event]
      Some(event)
    }).filter(event => !event.ip.contains("192.168") && b_keyword.value.contains(event.interfaceName)
    ).map(line => Tuple2(line.interfaceName + "_" + line.ip, newEvent(line.interfaceName,line.time, line.ip, 1)) )
      .reduceByKeyAndWindow(aggregateFunc, Durations.seconds(b_windowDuration.value.toInt),  Durations.seconds(b_slideDuration.value.toInt))
      .map(x=>x._2)
      .filter(filterFunc)
      .foreachRDD( rdd =>{
          rdd.foreachPartition(records =>{
              records.foreach(record => {
                logger.info("record: interface: " + record.interfaceName +"  date: " + record.time + "  ip: " + record.ip + " count: " + record.count)
                Process(record, b_keyMap.value)
              })
          })
        })

    ssc.start()
    ssc.awaitTermination()
  }
}
