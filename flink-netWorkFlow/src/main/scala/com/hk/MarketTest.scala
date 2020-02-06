package com.hk

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

/**
 * app渠道推广，分渠道
 */
object MarketTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimpleAdEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "uninstall")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1) //以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print()
    env.execute("-")
  }

  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
      val startTs = new Timestamp(context.window.getStart).toString
      val endTs = new Timestamp(context.window.getEnd).toString
      val channel = key._1
      val behavior = key._2
      val count = elements.size
      out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
    }
  }

}

//自定义数据源
class SimpleAdEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  //运行标志位
  var running = true

  //定义用户行为集合
  val behaviorTypes: Seq[String] = Seq("click", "download", "install", "uninstall")
  //定义取到集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  //随机数发生器
  val rand: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上限
    val maxElement = 100000;
    var count = 0
    while (running && count < maxElement) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes {
        rand.nextInt(behaviorTypes.size)
      }
      val chanel = channelSets {
        rand.nextInt(channelSets.size)
      }
      val ts = System.currentTimeMillis()
      sourceContext.collect(MarketingUserBehavior(id, behavior, chanel, ts))
      count += count
      TimeUnit.MILLISECONDS.sleep(50L)
    }
  }

  override def cancel(): Unit = running = false
}

