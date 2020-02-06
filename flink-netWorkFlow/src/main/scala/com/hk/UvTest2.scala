package com.hk

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{TriggerResult, Trigger}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */
object UvTest2 {

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
      .map(data => ("tempKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //触发器(Trigger)决定了窗口什么时候准备好被窗口函数处理。每个窗口分配器都带有一个默认的?Trigger。
      // 如果默认触发器不能满足你的要求，可以使用?trigger(...)?指定自定义的触发器。
      .trigger(new MyTrigger())
      .process(new MyProcess())

    dataStream.print()
    env.execute("-")

  }
}

class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult =
  //每来一条数据，直接触发窗口操作，并清除窗口状态
    TriggerResult.FIRE_AND_PURGE

  override def clear(w: TimeWindow, triggerContext: TriggerContext): Unit = {}

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: TriggerContext): TriggerResult = TriggerResult.CONTINUE
}

class Bloom(size: Long) extends Serializable {
  //位图的总大小
  private val cap = if (size > 0) size else 1 << 27

  //定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}

class MyProcess extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  lazy val jedis = new Jedis("localhost", 7379)
  lazy val bloom = new Bloom(1 << 29)

  @throws[Exception]
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }
    //用bloom过滤器判断是否已存在
    val userId = elements.last._2.toString
    val offSet = bloom.hash(userId, 71)

    val isExist = jedis.getbit(storeKey, offSet)

    if (isExist != 1) {
      jedis.setbit(storeKey, offSet, "1")
      jedis.hset("count", storeKey, (count + 1).toString)
    }
  }
}
