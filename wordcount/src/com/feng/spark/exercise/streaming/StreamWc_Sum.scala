package com.feng.spark.exercise.streaming

/**
  * Created by fengyanxin on 2019/4/3.
  */
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}



/**
  * Created by Administrator on 2017/10/10.
  * 功能：演示SparkStream中的wordcount的累加
  *
  */
object StreamWc_Sum {
  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc=(iter:Iterator[(String,Seq[Int],Option[Int])])=>{
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(m=>(x,m))}
  }


  def main(args: Array[String]): Unit = {
    //设置日志的级别
    //LoggerLevels.setStreamingLogLevels() //报错，暂时不能用不知道怎么回事
    val conf=new SparkConf().setAppName("StreamWc_Sum").setMaster("local[2]") //这里的setMaster就是本地开启两个线程的意思，
    // 至于为什么要开启两个线程是因为流处理要有一个线程持续接受数据而另一个线程对短暂存储的批量数据进行相应的计算处理
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    // ss做累加之前，一定要做checkpoint,而且存储的目录一定是hdfs
    //做checkpoint 写入共享存储中
    ssc.checkpoint("/test/streaming/wc_sum")
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

