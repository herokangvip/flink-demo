package com.hk

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 订单超时检测CEP实现
 */
object OrderCEP {
  def main(args: Array[String]): Unit = {
    //订单号，create/pay，结算id，时间戳
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/order.txt")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).trim.toLong, arr(1).trim, arr(2).trim, arr(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    //定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("a").where(_.eventType == "create")
      .followedBy("b").where(_.eventType == "pay")
      .within(Time.seconds(15))
    //在事件流上应用模式，得到一个pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)
    //调用select方法，提取时间序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("-")
  }
}

//超时事件序列处理函数,l是超时时的时间戳
class OrderTimeoutSelect extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val orderId = map.get("a").iterator().next().orderId
    OrderResult(orderId, "timeout")
  }
}

//正常支付事件处理
class OrderPaySelect extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId = map.get("a").iterator().next().orderId
    OrderResult(orderId, "payed success")
  }
}

case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class OrderResult(orderId: Long, msg: String)
