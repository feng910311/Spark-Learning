package com.feng.spark.exercise.basic

/**
  * Created by fengyanxin on 2019/4/2.
  */
import java.io.FileWriter
import java.io.File
import scala.util.Random

object AgeData {

  def main(args:Array[String]) {
    val writer = new FileWriter(new File("D:\\sample_age_data.txt"),false)
    val rand = new Random()
    for ( i <- 1 to 10000000) {
      writer.write( i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
