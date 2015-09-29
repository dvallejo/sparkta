/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparkta.driver.factory

import java.io.File
import java.net.URI

import akka.event.slf4j.SLF4JLogging
import com.stratio.sparkta.serving.core.{AppConstant, SparktaConfig}
import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.Try

object SparkContextFactory extends SLF4JLogging {

  private var sc: Option[SparkContext] = None
  private var sqlContext: Option[SQLContext] = None
  private var ssc: Option[StreamingContext] = None

  def sparkSqlContextInstance: Option[SQLContext] = {
    synchronized {
      sqlContext match {
        case Some(_) => sqlContext
        case None => if (sc.isDefined) sqlContext = Some(new SQLContext(sc.get))
      }
    }
    sqlContext
  }

  def sparkStreamingInstance: Option[StreamingContext] = ssc

  def sparkStreamingInstance(batchDuration: Duration, checkpointDir: String): Option[StreamingContext] = {
    val ssc = Option(StreamingContext.getOrCreate(checkpointDir, { () =>
      val ssc = new StreamingContext(sc.get, batchDuration)
      ssc.checkpoint(checkpointDir)
      ssc
    }))
    this.ssc = ssc
    ssc
  }

  def setSparkContext(createdContext: SparkContext): Unit = sc = Some(createdContext)

  private def getNewStreamingContext(batchDuration: Duration, checkpointDir: String): StreamingContext = {
    val ssc = new StreamingContext(sc.get, batchDuration)
    ssc.checkpoint(checkpointDir)
    ssc
  }

  def sparkStandAloneContextInstance(generalConfig: Option[Config],
                                     specificConfig: Map[String, String],
                                     jars: Seq[File]): SparkContext =
    synchronized {
      sc.getOrElse(instantiateStandAloneContext(generalConfig, specificConfig, jars))
    }

  def sparkClusterContextInstance(specificConfig: Map[String, String], jars: Seq[URI]): SparkContext =
    synchronized {
      sc.getOrElse(instantiateClusterContext(specificConfig, jars))
    }

  private def instantiateStandAloneContext(generalConfig: Option[Config],
                                           specificConfig: Map[String, String],
                                           jars: Seq[File]): SparkContext = {
    sc = Some(new SparkContext(configToSparkConf(generalConfig, specificConfig)))
    jars.foreach(f => sc.get.addJar(f.getAbsolutePath))
    sc.get
  }

  private def instantiateClusterContext(specificConfig: Map[String, String],
                                        jars: Seq[URI]): SparkContext = {
    sc = Some(new SparkContext(configToSparkConf(None, specificConfig)))
    jars.foreach(f => sc.get.addJar(f.toString))
    sc.get
  }

  private def configToSparkConf(generalConfig: Option[Config], specificConfig: Map[String, String]): SparkConf = {
    val conf = new SparkConf()
    if (generalConfig.isDefined) {
      val properties = generalConfig.get.entrySet()
      properties.foreach(e => {
        if (e.getKey.startsWith("spark."))
          conf.set(e.getKey, generalConfig.get.getString(e.getKey))
      })
    }
    specificConfig.foreach(e => conf.set(e._1, e._2))

    conf
  }

  def destroySparkStreamingContext: Unit = {
    synchronized {
      StreamingContext.getActive() match {
        case Some(ssc) =>
          val stopGracefully =
            Try(SparktaConfig.getDetailConfig.get.getBoolean(AppConstant.ConfigStopGracefully)).getOrElse(true)
          log.info(s"Stopping streamingContext with name: ${ssc.sparkContext.appName}")
          StreamingContext.getActive().get.stop(false, stopGracefully)
          log.info(s"Stopped streamingContext with name: ${ssc.sparkContext.appName}")

        case None =>
          log.warn("Cannot destroy Spark Streaming Context")
      }
    }
  }

  def destroySparkContext: Unit = {
    synchronized {
      destroySparkStreamingContext
      if (sc.isDefined) {
        log.debug("Stopping SparkContext with name: " + sc.get.appName)
        sc.get.stop()
        log.debug("Stopped SparkContext with name: " + sc.get.appName)
        sc = None
      }
    }
  }
}
