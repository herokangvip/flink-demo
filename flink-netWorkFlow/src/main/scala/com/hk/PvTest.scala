package com.hk

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PvTest {
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
        val array = data.split(",")
        UserBehavior(array(0).trim.toLong, array(1).trim.toLong, array(2).trim.toInt, array(3).trim, array(4).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000
      })
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print()
    env.execute("-")

  }

}
