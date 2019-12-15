package HaoXin.LearnSpark.SQL

import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadParquet {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Read Parquet")
      .master("local[4]")
      .config("spark.sql.adaptive.enabled","true")
      .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize","134217228")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    sparkSession.read.parquet("D:\\data\\sparktest\\e")
      .select("name","age","ID")
      .groupBy("name","ID")
      .agg(
        avg("age").as("age"),
        max("age").as("maxage"),
        min("age").as("minage")
      )
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .parquet("D:\\data\\sparktest\\d")
  }
}
