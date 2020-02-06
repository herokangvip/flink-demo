package com.hk

import java.util.concurrent.TimeUnit

import javax.swing.SwingWorker.StateValue
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 登录失败检测
 */
object LoginFailTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginEventStream = env.addSource(new SimpleLoginEventSource())
      .assignAscendingTimestamps(_.eventTime)

    val warningStream = loginEventStream
      .keyBy(_.userId) //以用户id分组
      .process(new LoginWarning(2))

    warningStream.print()
    env.execute("-")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //2秒内登录失败的个数
  lazy val loginFailState: ValueState[Long] = getRuntimeContext
    .getState(new ValueStateDescriptor[Long]("loginFailState", classOf[Long]))

  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    val loginFailList = loginFailState.value()
    //判断类型是否是fail，只添加fail到状态
    if (value.eventType == "fail") {
      if (loginFailList != 0) {
        context.timerService().registerEventTimeTimer(value.eventTime + 2000L)
      }
      loginFailState.update(loginFailList + 1)
    } else {
      loginFailState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //触发定时器时根据报警个数决定是否输出报警
    if (loginFailState.value() > maxFailTimes) {
      out.collect(Warning(ctx.getCurrentKey, "短时间内连续登录失败报警"))
    }
    //清空状态
    loginFailState.clear()
  }
}

//输入输出样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, warningMsg: String)


//自定义数据源
class SimpleLoginEventSource() extends RichSourceFunction[LoginEvent] {
  //运行标志位
  var running = true

  //userId: Long, ip: String, eventType: String, eventTime: Long
  //定义用户集合
  val userIds: Seq[Long] = Seq(11L, 12L, 13L, 14L, 15L)
  //定义省集合
  val ips: Seq[String] = Seq("192.168.232.01", "192.168.232.02", "192.168.232.03", "192.168.232.04", "192.168.232.05")
  //定义登录类型
  val types: Seq[String] = Seq("success", "fail")
  //随机数发生器
  val rand: Random = new Random()

  override def run(sourceContext: SourceFunction.SourceContext[LoginEvent]): Unit = {
    //定义一个生成数据的上限
    val maxElement = 100000;
    var count = 0
    while (running && count < maxElement) {
      val userId = userIds {
        rand.nextInt(userIds.size)
      }
      val ip = ips {
        rand.nextInt(ips.size)
      }
      val eventType = types {
        rand.nextInt(types.size)
      }

      val ts = System.currentTimeMillis()
      sourceContext.collect(LoginEvent(userId, ip, eventType, ts))
      count += count
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = running = false
}