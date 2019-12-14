package HaoXin.LearnSpark.Streaming

import java.awt.datatransfer.StringSelection

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ReadKafkaData {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("haoxin-test")
    val ssc: StreamingContext =new StreamingContext(conf,Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.65.130:9092,192.168.65.130:9093,192.168.65.130:9094",// kafka 集群
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "haoxin01",
      "auto.offset.reset" -> "latest",  // 给定第一次连接的时候，默认的consumer的offset值初始化为多少
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Seq("test")  //主题，可配置多个
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(a=>(a.key(),a.value()))
        .map(a=>a._2.split("\t"))
        .map(a=>(a(0),a(1),a(2),a(3),a(4),a(5),a(6)))
        .print()


    // 五、启动
    ssc.start()
    ssc.awaitTermination()
  }
}
