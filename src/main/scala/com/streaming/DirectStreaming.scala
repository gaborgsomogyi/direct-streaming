package com.streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class TestRecord(column1: Int, column2: Int)

object DirectStreaming {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val homeDir = System.getProperty("user.home")
    val jobDataDir = homeDir + "/jobdata"
    val inDir = jobDataDir + "/in"
    val outDir = jobDataDir + "/out/data"
    val checkpointDir = jobDataDir + "/checkpoint"

    FileUtils.deleteDirectory(new File(outDir))
    FileUtils.deleteDirectory(new File(checkpointDir))

    log.info("Creating streaming context...")
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    log.info("OK")

    val stream = ssc.textFileStream(inDir)
    stream.saveAsTextFiles(outDir)
    ssc.checkpoint(checkpointDir)
    ssc.start()

    log.info("Waiting for termination...")
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    log.info("OK")
  }
}
