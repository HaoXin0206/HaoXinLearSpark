package HaoXin.LearnSpark.SQL

import org.apache.spark.sql.SparkSession

object ReadParquet {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Read Parquet")
      .master("local[4]").getOrCreate()
    sparkSession.read.parquet("")
      .show()
  }
}
