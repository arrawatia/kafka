/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.proxy

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import kafka.admin._
import kafka.cluster.EndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.network.SocketServer
import kafka.security.auth.Authorizer
import kafka.server.{KafkaApis, KafkaConfig, KafkaRequestHandlerPool}
import kafka.utils.{CoreUtils, KafkaScheduler, Logging, Mx4jLoader, SystemTime, Time, ZkUtils}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, _}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.AppInfoParser

/**
  * Represents the lifecycle of a single Kafka broker. Handles all functionality required
  * to start up and shutdown a single Kafka node.
  */
class KafkaProxyServer(val config: KafkaConfig, time: Time = SystemTime, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  private val jmxPrefix: String = "kafka.server"
  private val reporters: java.util.List[MetricsReporter] = config.metricReporterClasses
  reporters.add(new JmxReporter(jmxPrefix))

  // This exists because the Metrics package from clients has its own Time implementation.
  // SocketServer/Quotas (which uses client libraries) have to use the client Time objects without having to convert all of Kafka to use them
  // Eventually, we want to merge the Time objects in core and clients
  private implicit val kafkaMetricsTime: org.apache.kafka.common.utils.Time = new org.apache.kafka.common.utils.SystemTime()
  var metrics: Metrics = null

  private val metricConfig: MetricConfig = new MetricConfig()
    .samples(config.metricNumSamples)
    .timeWindow(config.metricSampleWindowMs, TimeUnit.MILLISECONDS)

  var apis: KafkaApisProxy = null
  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null

  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
  import scala.collection.JavaConversions.mapAsScalaMap
//  val proxyClient =  AdminClient.create(mapAsScalaMap(config.originals()).toMap)
  val proxyClient =  AdminClient.createSimplePlaintext("localhost:9092")

  var zkUtils: ZkUtils = null


  /**
    * Start up API for bringing up a single instance of the Kafka server.
    * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
    */
  def startup() {
    try {
      info("starting")

      if(isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if(startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        metrics = new Metrics(metricConfig, reporters, kafkaMetricsTime, true)


        /* start scheduler */
        kafkaScheduler.startup()

        socketServer = new SocketServer(config, metrics, kafkaMetricsTime)
        socketServer.startup()

        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
          val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
          authZ.configure(config.originals())
          authZ
        }

        /* start processing requests */
        apis = new KafkaApisProxy(socketServer.requestChannel, config.brokerId, config, metrics, authorizer, proxyClient)
        requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)

        Mx4jLoader.maybeLoad()

        /* tell everyone we are alive */
        val listeners = config.advertisedListeners.map {case(protocol, endpoint) =>
          if (endpoint.port == 0)
            (protocol, EndPoint(endpoint.host, socketServer.boundPort(protocol), endpoint.protocolType))
          else
            (protocol, endpoint)
        }

        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }


  /**
    * Shutdown API for shutting down a single instance of the Kafka server.
    * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
    */
  def shutdown() {
    try {
      info("shutting down")

      if(isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      val canShutdown = isShuttingDown.compareAndSet(false, true)
      if (canShutdown && shutdownLatch.getCount > 0) {
        if(socketServer != null)
          CoreUtils.swallow(socketServer.shutdown())
        if(requestHandlerPool != null)
          CoreUtils.swallow(requestHandlerPool.shutdown())
        CoreUtils.swallow(kafkaScheduler.shutdown())
        if(apis != null)
          CoreUtils.swallow(apis.close())
        CoreUtils.swallow(authorizer.foreach(_.close()))
        if (metrics != null)
          CoreUtils.swallow(metrics.close())

        startupComplete.set(false)
        isShuttingDown.set(false)
        AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
    * After calling shutdown(), use this API to wait until the shutdown is complete
    */
  def awaitShutdown(): Unit = shutdownLatch.await()


  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = socketServer.boundPort(protocol)








}
