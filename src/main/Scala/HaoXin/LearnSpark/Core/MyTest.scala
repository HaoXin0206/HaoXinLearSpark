package HaoXin.LearnSpark.Core

import org.apache.spark.{SparkConf, SparkContext}

object MyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("haoxin-test")

    val context = SparkContext.getOrCreate(conf)

    context.parallelize(Seq(
      Array(1,2,3,4),
      Array(2,3,4,5),
      Array(3,4,5,6),
      Array(4,5,6,7)
    ))
      .flatMap(a=>a)
      .map(a=>(a,1))
      .reduceByKey(_+_)
      .sortByKey()
      .foreach(println)


  }

}
