package HaoXin.LearnSpark.Core

import java.text.DecimalFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WriteDataToLocalHost {
  def main(args: Array[String]): Unit = {
    val context = new SparkConf()
      .setAppName("")
      .setMaster("local[*]")

    val sc=SparkContext.getOrCreate(context)

    val ss=sc.textFile("D:\\TestData\\20190729")
      .map(a=>{
        val format = new DecimalFormat("00")
        val strings = a.replace("(","").replace(")","").split(",")
        val hour = strings(0).substring(11,13)
        val minute1 = strings(0).substring(14,16).replace(":","").toInt
        val minute= format.format((minute1/15)*15)
//        val minute=strings(0).substring(14,16)
        (s"${hour}:${minute}",strings(1),strings(2))
      })
//      .foreach(println)
      .saveAsTextFile("D:\\TestData\\20190729-out")


  }

}
