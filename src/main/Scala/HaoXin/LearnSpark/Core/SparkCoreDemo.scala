package HaoXin.LearnSpark.Core

import org.apache.spark.sql.SparkSession

object SparkCoreDemo {
  def main(args: Array[String]): Unit = {
    val sc=SparkSession
      .builder()
      .appName("")
      .master("local[4]")
      .getOrCreate()
  }

}
