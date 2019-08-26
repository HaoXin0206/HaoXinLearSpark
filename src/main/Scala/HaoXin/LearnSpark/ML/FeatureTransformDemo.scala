package HaoXin.LearnSpark.ML

import org.apache.spark.ml.feature.{Binarizer, MinMaxScaler, Normalizer, OneHotEncoder, PolynomialExpansion, StandardScaler, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FeatureTransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("")
      .setMaster("local[4]")

    val context = SparkContext.getOrCreate(conf)
    val sQLContext = new SQLContext(context)

    val frame = sQLContext.createDataFrame(Seq(
      (0, 1.0),
      (1, 3.0),
      (2, 3.3),
      (3, 2.3)
    )).toDF("id", "value")


    /*二值化，当value的值超过给定数，返回1，否则返回0*/
    val binarizer = new Binarizer()
      .setInputCol("value")
      .setOutputCol("bi_value")
      .setThreshold(2.5)
    binarizer.transform(frame).show(false)

    /*多项式扩充*/
    val df2 = sQLContext.createDataFrame(Seq(
      Vectors.dense(2.0, 3.0),
      Vectors.dense(3.0, 2.3),
      Vectors.dense(4.0, 3.9)
    ).map(a => Tuple1.apply(a))).toDF("value")
    df2.show(false)

    val poly=new PolynomialExpansion()
      .setInputCol("value")
      .setOutputCol("poly_value")
      .setDegree(2) //多项式的次数
    poly.transform(df2).show(false)

    //压编码
    val df3=sQLContext.createDataFrame(Seq(
      (1,"a"),
      (2,"b"),
      (3,"c"),
      (4,"d"),
      (5,"a"),
      (6,"b")
    )).toDF("id","cate")

    val indexer = new StringIndexer()
      .setInputCol("cate")
      .setOutputCol("haoxin_cate")
      .setHandleInvalid("skip")  //设置异常处理方式
    val model = indexer.fit(df3)//构建模型
    val df4=model.transform(df3)
    val encode=new OneHotEncoder()
      .setInputCol("haoxin_cate")
      .setOutputCol("ca_haoxin")
      .setDropLast(false) //是否删除最后一个类别,即最后一列，默认删除
    encode.transform(df4).show(false)


    //归一化处理
    val df5=sQLContext.createDataFrame(Seq(
      Vectors.dense(2,3.0,4.0,4.0),
      Vectors.dense(3.0,4.0,5.0,2.0),
      Vectors.dense(4.0,5.0,6.0,2.5)
    ).map(a=>Tuple1.apply(a))).toDF("value")
    val normalizer=new Normalizer()
      .setInputCol("value")
      .setOutputCol("aa_value")
      .setP(2) //给定求sum方式 1：求绝对值  2：求绝对值平方和 p表示绝对值的p次方和，如果是无穷大(Double.PositiveInfinity)，表示特征属性的最大值

    normalizer.transform(df5).show(false)

    val scal1=new StandardScaler()
      .setInputCol("value")
      .setOutputCol("aa_value_01")
      .setWithMean(true) //是否将特征属性转换为均值为0的向量  默认false
      .setWithStd(true)//是否将特征属性转换为均值为1的向量  默认true
    val models = scal1.fit(df5)
    models.transform(df5).show(false)


    /*缩放的方式进行归一化操作*/
    val scal2=new MinMaxScaler()
      .setInputCol("value")
      .setOutputCol("ha_value")
      .setMin(0) //设置返回最小值，默认0
      .setMax(1) //设置返回的最大值，默认1
    val model02 = scal2.fit(df5)
    model02.transform(df5).show(false)

      //Vector合并
    val df6=sQLContext.createDataFrame(Seq(
      (1,"a",Vectors.dense(12,1.0)),
      (1,"b",Vectors.dense(2,1.0)),
      (1,"a",Vectors.dense(11,1.0)),
      (1,"c",Vectors.dense(12,21.0)),
      (1,"d",Vectors.dense(1.2,1.0))
    )).toDF("id","cate","value")


  }

}
