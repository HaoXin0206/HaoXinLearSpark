package HaoXin.LearnSpark.Core


import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkCoreDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","haoxin")
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
      .cache()

    println(rdd.getNumPartitions)
    val l1=System.currentTimeMillis()
      rdd.foreach(a=>{
        println(a)
        saveDataToHDfsFile(  a._2.toList,a._1+"")
      })

    val l2=System.currentTimeMillis()
    println(l2-l1)

    println("===============>")
    rdd.collect().toList
      .foreach(a=>{
        saveDataToHDfsFile(a._2.toList,a._1+"")
        println(a)
      })

    println(System.currentTimeMillis()-l2)

    rdd.unpersist()

  }

  def saveDataToHDfsFile(data:List[String],name:String): Unit ={
    val conf=new Configuration()
    conf.set("fs.defaultFS","hdfs://haoxin01:8020")
    val fileSystem=FileSystem.get(conf)
    val path=new Path(s"hdfs://haoxin01:8020/haoxin/data/2019063003/${name}")
    if (fileSystem.exists(path)) fileSystem.deleteOnExit(path)
    val input: FSDataOutputStream =fileSystem.create(path)
    data.foreach(a=>{
      input.writeBytes(a+"\n")
    })
    input.flush()
    input.close()

  }

}
