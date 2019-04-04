package com.feng.spark.exercise.streaming

/**
  * Created by fengyanxin on 2019/4/3.
  * 功能：演示SparkStream中的wordcount的累加
  */

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object StreamWork_Sum_His {
  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc=(iter:Iterator[(String,Seq[Int],Option[Int])])=>{  //第二个参数是列表类型 也就是Seq类型
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(m=>(x,m))}  //getOrElse函数是这样的，如果z的值不为空，则返回z的值，否则返回括号里的默认值0，
  }  //定义一个函数变量，入参是一个迭代器类型的参数，其中迭代器中的每一个元素是一个三元组，三元组分别为String Seq[Int] Int
     //然后函数的操作是将三元组英社称一个二元的组 String Int
      //也就是说这个函数的逻辑是将第二个参数中的原色全部相加，然后加上第三个参数，与第一个参数组成一个二元组，形成新的元素。

  def main(args: Array[String]): Unit = {
    //设置日志的级别
    //LoggerLevels.setStreamingLogLevels() 不好使
    val conf=new SparkConf().setAppName("StreamWc_Sum").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    // ss做累加之前，一定要做checkpoint,而且存储的目录一定是hdfs
    //做checkpoint 写入共享存储中
    ssc.checkpoint("/test/streaming/wc_sum_his")
    val lines=ssc.socketTextStream("10.4.122.14",8888)
    //没有累加的计数
    //    val result=lines.flatMap(_.split(" ")).map((_,1)).reduceByKey()
    // 用默认的hash分区, true表示以后也这样用的
    //updateStateByKey结果可以累加但是需要传入一个自定义的累加函数：updateFunc  三个参数
    val result=lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    result.print()

    //启动sparkstream
    ssc.start()
    ////等待结束任务
    ssc.awaitTermination()

  }
}
