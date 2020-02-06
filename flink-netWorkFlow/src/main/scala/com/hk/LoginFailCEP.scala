package com.hk

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * CEP:complex event processing,复杂事件处理
 * 连续登录失败检测
 */
object LoginFailCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginEventStream = env.addSource(new SimpleLoginEventSource())
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId) //以用户id分组

    //只要控制好waterMark，可以检测到乱序数据
    //定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))
    //在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)
    //从patternStream上应用selectFunction检出匹配事件序列
    val loginFailDateStream = patternStream.select(new LoginFailMatch())


    loginFailDateStream.print()
    env.execute("-")
  }

  class LoginFailMatch extends PatternSelectFunction[LoginEvent, Warning] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
      //从map中按名称取出对应的事件
      val firstFail = map.get("begin").iterator().next()
      val lastFail = map.get("next").iterator().next()
      Warning(firstFail.userId, "第一次失败时间：" + firstFail.eventTime + "，第二次失败时间：" + lastFail.eventTime)
    }
  }

}
