package HaoXin.LearnSpark.Core


import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkCoreDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[4]")
      .setAppName("")


    val sc=SparkContext.getOrCreate(conf)

    val tes=sc.broadcast(List("(",")",".",":","=","+","-",",",">","<","*",":","：","，"," "," ","\t"))

    val rdd=sc.textFile("D:\\资料\\资料\\SaprkMlib\\03_随堂笔记")
      .flatMap(a=>a.split(""))
      .map(a=>{
        var data=a
        tes.value.foreach(ss=>{
          if (a.equals(ss)) data=""
        })
        data
      })
      .filter(_.nonEmpty)
      .map(a=>(a.toUpperCase,1))
      .reduceByKey(_+_)
      .map(a=>a.swap)
      .groupByKey()
      .foreach(a=>{
        println(a)
        saveDataToHDfsFile(  a._2.toList,a._1+"")
      })

  }

  def saveDataToHDfsFile(data:List[String],name:String): Unit ={
    val conf=new Configuration()
    conf.set("fs.defaultFS","hdfs://haoxin01:8020")
    val fileSystem=FileSystem.get(conf)
    val input: FSDataOutputStream =fileSystem.create(new Path(s"hdfs://haoxin01:8020/haoxin/data/2019063003/${name}"))
    data.foreach(a=>{
      input.writeBytes(a+"\n")
    })
    input.flush()
    input.close()

  }

}
