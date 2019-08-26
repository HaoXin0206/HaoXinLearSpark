package HaoXin.LearnSpark.ML

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TextSplitProcess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("")
      .setMaster("local[4]")

    val context = SparkContext.getOrCreate(conf)
    val sqlContext = new SQLContext(context)

    val df=sqlContext.createDataFrame(Seq(
      (0,"hi haoxin"),
      (1,"I think you can do well"),
      (2,"why"),
      (3,"because you are Best")
    )).toDF("lable","sectence")
    df.show(false)

    //基于空格的划分
    val tokenizer=new Tokenizer()
      .setInputCol("sectence") //给定转换的输入列
      .setOutputCol("haoxin_sec")//给定转换结果的输出列

    val df1=tokenizer.transform(df)  //对DataFrame对象进行转换
    df1.show(false)

    //基于正则划分
    val regexTokenizer=new RegexTokenizer()
      .setMinTokenLength(2)//给定单词长度的下限（不包含下线）
      .setInputCol("sectence")
      .setOutputCol("haoxin_ss")
      .setPattern("\\W") //默认是\\s+,给定分割字符串匹配的正则或者是获取数据的正则，等价于.setPattern("\\w+") .setGaps(false)
    // ；当gaps设置成false，表示给定的正则是数据匹配的正则，当设置成true时，表示给定的正则是表示的分割字符串，默认是true
      .setToLowercase(false)//是否转为小写

    val df2=regexTokenizer.transform(df)
    df2.show(false)

    //去掉停止词
    val remover=new StopWordsRemover()
      .setInputCol("haoxin_ss")
      .setOutputCol("filter_word")
//      .setStopWords(Array(""))//给定停止词

    val df3 = remover.transform(df2)
    df3.show(false)

    //给定类别序列号
    val df4 = sqlContext.createDataFrame(Seq(
      (10, "a"), (11, "b"), (21, "c"), (13, "d"),
      (41, "b"), (15, "a"), (16, "a"), (71, "f")
    )).toDF("id", "category")
    val df5 = sqlContext.createDataFrame(Seq(
      (10, "a"), (11, "b"), (21, "c"), (13, "d"),
      (41, "b"), (15, "a"), (16, "a"), (71, "f"),
      (8, "g")
    )).toDF("id", "category")


    val indexer=new StringIndexer()
      .setInputCol("category")
      .setOutputCol("category_id")
      .setHandleInvalid("skip") //当模型构建号后，如果进行转换的Dataframe对应的输入列中，类别不存在，采用的处理方式，默认是error（报错），可以修改为skip（过滤）
    //构建模型
    val indexerModel=indexer.fit(df4)
    //构建模型后对已有/未知数据进行转换
    indexerModel.transform(df4).show(false)
    indexerModel.transform(df5).show(false)

  }

}
