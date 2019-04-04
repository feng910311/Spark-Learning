package com.feng.spark.exercise.basic

/**
  * Created by fengyanxin on 2019/4/2.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object AvgAge {
  def main(args:Array[String]) {
    if (args.length < 1){
      println("Usage:AvgAgeCalculator datafile")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark Exercise:Average Age Calculator")
    val sc = new SparkContext(conf)
    val dataFile = sc.textFile(args(0), 5);
    val count = dataFile.count()
    val ageData = dataFile.map(line => line.split(" ")(1))
    val totalAge = ageData.map(age => Integer.parseInt(
      String.valueOf(age))).collect().reduce((a,b) => a+b)
    println("Total Age:" + totalAge + ";Number of People:" + count )
    val avgAge : Double = totalAge.toDouble / count.toDouble
    println("Average Age is " + avgAge)
  }
}