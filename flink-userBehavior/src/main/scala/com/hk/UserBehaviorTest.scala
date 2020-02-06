package com.hk

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Description: 
  *
  * @author heroking
  * @version 1.0.0
  */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object UserBehaviorTest {
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
    //flink checkpoint偏移量状态也会保存，FlinkKafkaConsumer封装了保存offset的功能
    val consumer011 = new FlinkKafkaConsumer011[String]("test-topic", new SimpleStringSchema(), prop)
    val dataStream = env.addSource(consumer011)
      .map(data => {
        val array = data.split(",")
        UserBehavior(array(0).trim.toLong, array(1).trim.toLong, array(2).trim.toInt, array(3).trim, array(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    //transfer
    val pvStream = dataStream.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //.aggregate(new CountAgg())
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopHotItems(3))

    pvStream.print()

    env.execute("test")
  }

}

//预聚合
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc

  override def add(in: UserBehavior, acc: Long): Long = acc + 1
}

class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每条数据存入状态列表
    itemState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发时对所有数据排序并输出结果
    //将所有数据取出放到ListBuffer
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]

    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    //按照点击量大小排序,并取前n个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemState.clear()
    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val current = sortedItems(i)
      result.append("No:").append(i)
        .append("商品id:").append(current.itemId)
        .append("浏览量:").append(current.count)
        .append("\n")
    }
    result.append("=============")
    Thread.sleep(1000L)
    out.collect(result.toString())
  }

}
