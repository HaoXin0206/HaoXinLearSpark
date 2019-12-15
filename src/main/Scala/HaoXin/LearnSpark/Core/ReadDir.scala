package HaoXin.LearnSpark.Core

import java.io.{File, FileWriter}

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object ReadDir {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Read Dir File").setMaster("local[4]")

    val sc = SparkContext.getOrCreate(conf)

    val dd = sc.binaryFiles("D:\\data\\sparktest\\e")
      .groupBy(a => a._1)
      .flatMap(a => {
        val filename = a._1

        val value: List[((String, String), Int)] = a._2.flatMap(data => data._2.toArray())
          .map(data => data.toChar.toString)
          .filter(data => data.replace(" ","").length!=0)
          .map(data => ((filename, data), 1))
          .toList


        value
      })
      .reduceByKey(_ + _)
      .map(a => (a._1._1, (a._1._2, a._2)))
      .groupByKey()
      .mapPartitions(data => {
        data.map(d => {
          val filename = d._1.split("/").last
//          val file = new File(s"D:\data\sparktest\d\\${filename}")
//          val writer = new FileWriter(file)
//
//          val list = d._2.toList
//         list.foreach(a=>{
//           writer.write(s"${a._1}\t${a._2}")
//           writer.flush()
//         })
//
//          writer.close()

          filename
        })
      })
      .foreach(a => {
        println(a)
      })


  }

  def GetFilePath(dirPath: String): List[String] = {
    val file_list: ListBuffer[String] = ListBuffer[String]()
    val file = new File(dirPath)
    for (name <- file.listFiles()) {
      if (name.isDirectory) {
        file_list += name.getPath
      }
    }

    file_list.toList
  }

}
