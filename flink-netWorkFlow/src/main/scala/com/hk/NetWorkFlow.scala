package com.hk

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ListState}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Description: 实时网络流量统计
  *
  * @author heroking
  * @version 1.0.0
  */

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlWindowCount(url: String, windowEnd: Long, count: Long)

object NetWorkFlow {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("group.id", "test-group")
    prop.setProperty("key-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value-deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("auto.offset.reset", "latest")
    val consumer011 = new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop)
    val dataStream = env.addSource(consumer011)
      .map(data => {
        val array = data.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timstamp = sdf.parse(array(3).trim)
        //45.367.56.89 - - 17/05/2019:10:05:03 +0000 GET /index
        ApacheLogEvent(array(0).trim, array(1).trim, timstamp.getTime, array(6).trim, array(7).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new NetWorkFlowResultProcessFunction(3))

    dataStream.print()
    env.execute("--")
  }
}

class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
}

class WindowResult extends WindowFunction[Long, UrlWindowCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlWindowCount]): Unit = {
    out.collect(UrlWindowCount(key, window.getEnd, input.iterator.next()))
  }
}

class NetWorkFlowResultProcessFunction(topSize: Int) extends KeyedProcessFunction[Long, UrlWindowCount, String] {
  lazy val urlState: ListState[UrlWindowCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlWindowCount]("urlState", classOf[UrlWindowCount]))

  override def processElement(input: UrlWindowCount, context: KeyedProcessFunction[Long, UrlWindowCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(input)
    context.timerService().registerEventTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlWindowCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allList: ListBuffer[UrlWindowCount] = new ListBuffer[UrlWindowCount]()
    val iter = urlState.get().iterator()
    while (iter.hasNext) {
      allList.append(iter.next())
    }

    urlState.clear()

    val sortedItems = allList.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val current = sortedItems(i)
      result.append("No:").append(i + 1)
        .append(",url:").append(current.url)
        .append(",浏览量:").append(current.count)
        .append("\n")
    }
    result.append("=============")
    Thread.sleep(1000L)
    out.collect(result.toString())
  }
}
