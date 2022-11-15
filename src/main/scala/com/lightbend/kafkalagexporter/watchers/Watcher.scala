/*
 * Copyright (C) 2018-2022 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2022 Sean Glover <https://seanglover.com>
 */

package com.lightbend.kafkalagexporter.watchers

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.ActorContext
import com.lightbend.kafkalagexporter.{
  AppConfig,
  ConduktorWatcherConfig,
  KafkaCluster,
  KafkaClusterManager
}
import io.conduktor.common.circe.SubConfiguration

import scala.concurrent.ExecutionContext

object Watcher {

  sealed trait Message
  sealed trait Stop extends Message
  final case object Stop extends Stop

  trait Client {
    def close(): Unit
  }

  trait Events {
    def added(cluster: KafkaCluster): Unit
    def removed(cluster: KafkaCluster): Unit
    def error(e: Throwable): Unit
  }

  def createClusterWatchers(
      context: ActorContext[KafkaClusterManager.Message],
      appConfig: AppConfig
  )(nonBlockingIOEc: ExecutionContext): Seq[ActorRef[Watcher.Message]] = {
    // Add additional watchers here..
    val configMap = Seq(
      StrimziClusterWatcher.name -> appConfig.strimziWatcher,
      ConduktorWatcher.name -> appConfig.conduktorWatcher
    )
    configMap.flatMap {
      case (
            ConduktorWatcher.name,
            SubConfiguration.Enabled(config: ConduktorWatcherConfig)
          ) =>
        context.log.info(s"Adding watcher: ${ConduktorWatcher.name}")
        Seq(
          context.spawn(
            ConduktorWatcher.init(context.self, config)(nonBlockingIOEc),
            s"conduktor-cluster-watcher-${ConduktorWatcher.name}"
          )
        )
      case (StrimziClusterWatcher.name, true) =>
        context.log.info(s"Adding watcher: ${StrimziClusterWatcher.name}")
        Seq(
          context.spawn(
            StrimziClusterWatcher.init(context.self),
            s"strimzi-cluster-watcher-${StrimziClusterWatcher.name}"
          )
        )
      case _ => Seq()
    }
  }
}
