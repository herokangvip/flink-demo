package com.hk

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 广告点击统计,带黑名单,结合侧输出流输出报警信息
 */
object AdTest {
  //定义侧输出流的tag
  val blackListOutPutTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimpleEventSource())
      .assignAscendingTimestamps(_.timestamp)

    //自定义process，过滤大量刷点击的行为,同一个用户点同一个广告超过一定次数
    val filterBlackListStream = dataStream
      .keyBy(data => {
        (data.userId, data.adId)
      })
      .process(new FilterBlackListUser(100))

    val resultDataStream = filterBlackListStream
      .keyBy(_.province) //根据省份分组开窗聚合
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    resultDataStream.print("count")
    filterBlackListStream.getSideOutput(blackListOutPutTag).print("black")
    env.execute("-")
  }


  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    //定义状态保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
    //是否加入过黑名单的状态
    lazy val isSendBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSendBlackList", classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimer", classOf[Long]))

    override def processElement(input: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      //取出count状态
      val curCount = countState.value()
      if (curCount == 0) {
        val ts = (context.timerService().currentProcessingTime() / (60 * 60 * 24000) + 1) * (60 * 60 * 24000)
        resetTimer.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }
      //判断计数是否达到上限
      if (curCount >= maxCount) {
        //判断是否发送过黑名单，只发送一次
        if (!isSendBlackList.value()) {
          isSendBlackList.update(true)
          //输出到侧输出流
          context.output(blackListOutPutTag, BlackListWarning(input.userId, input.adId, "点击超过每日上限" + maxCount))
        }
        return
      } else {
        //计数状态+1，输出到主流
        countState.update(curCount + 1)
        collector.collect(input)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //清空状态
      if (timestamp == resetTimer.value()) {
        isSendBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }


}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L //初始值

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1 //一条新数据如何处理

  override def getResult(acc: Long): Long = acc //返回结果

  override def merge(acc: Long, acc1: Long): Long = acc + acc1 //不同分区数据组合
}

class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}

//输入的广告点击样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//按省统计的输出样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

//输出的黑名单报警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

//自定义数据源
class SimpleEventSource() extends RichSourceFunction[AdClickEvent] {
  //运行标志位
  var running = true

  //userId: Long, adId: Long, province: String, city: String,
  //定义用户集合
  val userIds: Seq[Long] = Seq(11, 12, 13, 14, 15)
  //定义广告id集合
  val adIds: Seq[Long] = Seq(10011, 10012, 10013, 10014, 10015)
  //定义省集合
  val provinces: Seq[String] = Seq("黑龙江", "河北", "广东", "山西", "内蒙古")
  //定义市集合
  val cities: Seq[String] = Seq("石家庄", "西安", "杭州", "郑州", "哈尔滨")
  //随机数发生器
  val rand: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[AdClickEvent]): Unit = {
    //定义一个生成数据的上限
    val maxElement = 100000;
    var count = 0
    while (running && count < maxElement) {
      val userId = userIds {
        rand.nextInt(userIds.size)
      }
      val adId = adIds {
        rand.nextInt(adIds.size)
      }
      val province = provinces {
        rand.nextInt(provinces.size)
      }
      val city = cities {
        rand.nextInt(cities.size)
      }

      val ts = System.currentTimeMillis()
      sourceContext.collect(AdClickEvent(userId, adId, province, city, ts))
      sourceContext.collect(AdClickEvent(10, 10010, "北京", city, ts))
      count += count
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}