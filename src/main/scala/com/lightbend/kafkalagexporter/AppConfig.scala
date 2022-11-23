/*
 * Copyright (C) 2018-2022 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2022 Sean Glover <https://seanglover.com>
 */

package com.lightbend.kafkalagexporter

import com.lightbend.kafkalagexporter.ConduktorWatcherConfig.MachineToMachine

import java.util
import eu.timepit.refined.auto._
import com.lightbend.kafkalagexporter.EndpointSink.ClusterGlobalLabels
import com.typesafe.config.{Config, ConfigObject}
import eu.timepit.refined
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.conduktor.api.common.dtos.{AuthToken, OrganizationId}
import io.conduktor.common.circe.SubConfiguration
import io.conduktor.primitives.types.Secret
import sttp.client3.UriContext
import sttp.model.Uri

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

object AppConfig {
  implicit class ConfigOps(config: Config) {
    def asNonEmpty(key: String): Refined[String, NonEmpty] =
      refined
        .refineV[NonEmpty]
        .apply(config.getString(key))
        .getOrElse(
          throw new IllegalArgumentException(s"$key must be non empty")
        )

    def asUrl(key: String): Uri =
      Uri.unsafeParse(config.getString(key))
  }

  def apply(config: Config): AppConfig = {
    val c = config.getConfig("kafka-lag-exporter")
    val pollInterval = c.getDuration("poll-interval").toScala

    val metricWhitelist = c.getStringList("metric-whitelist").asScala.toList

    val sinks =
      if (c.hasPath("sinks"))
        c.getStringList("sinks").asScala.toList
      else
        List("PrometheusEndpointSink")

    val sinkConfigs: List[SinkConfig] = sinks.flatMap { sink =>
      sink match {
        case "PrometheusEndpointSink" =>
          Some(new PrometheusEndpointSinkConfig(sink, metricWhitelist, c))
        case "InfluxDBPusherSink" =>
          Some(new InfluxDBPusherSinkConfig(sink, metricWhitelist, c))
        case "GraphiteEndpointSink" =>
          Some(new GraphiteEndpointConfig(sink, metricWhitelist, c))
        case _ => None
      }
    }

    val lookupTable = LookupTableConfig(c)
    val clientGroupId = c.getString("client-group-id")
    val kafkaClientTimeout = c.getDuration("kafka-client-timeout").toScala
    val kafkaRetries = c.getInt("kafka-retries")
    val clusters =
      c.getConfigList("clusters").asScala.toList.map { clusterConfig =>
        val consumerProperties =
          if (clusterConfig.hasPath("consumer-properties"))
            parseKafkaClientsProperties(
              clusterConfig.getConfig("consumer-properties")
            )
          else
            Map.empty[String, String]
        val adminClientProperties =
          if (clusterConfig.hasPath("admin-client-properties"))
            parseKafkaClientsProperties(
              clusterConfig.getConfig("admin-client-properties")
            )
          else
            Map.empty[String, String]
        val labels =
          Try {
            val labels = clusterConfig.getConfig("labels")
            labels
              .entrySet()
              .asScala
              .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
              .toMap
          }.getOrElse(Map.empty[String, String])

        val groupWhitelist =
          if (clusterConfig.hasPath("group-whitelist"))
            clusterConfig.getStringList("group-whitelist").asScala.toList
          else KafkaCluster.GroupWhitelistDefault

        val groupBlacklist =
          if (clusterConfig.hasPath("group-blacklist"))
            clusterConfig.getStringList("group-blacklist").asScala.toList
          else KafkaCluster.GroupBlacklistDefault

        val topicWhitelist =
          if (clusterConfig.hasPath("topic-whitelist"))
            clusterConfig.getStringList("topic-whitelist").asScala.toList
          else KafkaCluster.TopicWhitelistDefault

        val topicBlacklist =
          if (clusterConfig.hasPath("topic-blacklist"))
            clusterConfig.getStringList("topic-blacklist").asScala.toList
          else KafkaCluster.TopicBlacklistDefault

        KafkaCluster(
          clusterConfig.getString("name"),
          clusterConfig.getString("bootstrap-brokers"),
          groupWhitelist,
          groupBlacklist,
          topicWhitelist,
          topicBlacklist,
          consumerProperties,
          adminClientProperties,
          labels
        )
      }
    val strimziWatcher = c.getString("watchers.strimzi").toBoolean

    val conduktorWatcherConfig = if (c.hasPath("watchers.conduktor")) {
      val conduktor: Config = c.getConfig("watchers.conduktor")
      val subConf =
        if (conduktor.getBoolean("enabled"))
          SubConfiguration.Enabled[ConduktorWatcherConfig] _
        else SubConfiguration.Disabled[ConduktorWatcherConfig] _
      subConf(
        ConduktorWatcherConfig(
          adminApiUrl = conduktor.asUrl("admin-api-url"),
          machineToMachine = MachineToMachine(
            secret = Secret(conduktor.asNonEmpty("m2m_auth.shared_secret")),
            issuer = conduktor.asUrl("m2m_auth.issuer")
          )
        )
      )
    } else {
      SubConfiguration.Undefined
    }

    AppConfig(
      pollInterval,
      lookupTable,
      sinkConfigs,
      clientGroupId,
      kafkaClientTimeout,
      kafkaRetries,
      clusters,
      strimziWatcher,
      conduktorWatcherConfig
    )
  }

  // Copied from Alpakka Kafka
  // https://github.com/akka/alpakka-kafka/blob/v1.0.5/core/src/main/scala/akka/kafka/internal/ConfigSettings.scala
  def parseKafkaClientsProperties(config: Config): Map[String, String] = {
    @tailrec
    def collectKeys(
        c: ConfigObject,
        processedKeys: Set[String],
        unprocessedKeys: List[String]
    ): Set[String] =
      if (unprocessedKeys.isEmpty) processedKeys
      else {
        c.toConfig.getAnyRef(unprocessedKeys.head) match {
          case o: util.Map[_, _] =>
            collectKeys(
              c,
              processedKeys,
              unprocessedKeys.tail ::: o
                .keySet()
                .asScala
                .toList
                .map(unprocessedKeys.head + "." + _)
            )
          case _ =>
            collectKeys(
              c,
              processedKeys + unprocessedKeys.head,
              unprocessedKeys.tail
            )
        }
      }

    val keys = collectKeys(
      config.root,
      Set.empty[String],
      config.root().keySet().asScala.toList
    )
    keys.map(key => key -> config.getString(key)).toMap
  }

  def getPotentiallyInfiniteDuration(
      underlying: Config,
      path: String
  ): Duration = underlying.getString(path) match {
    case "infinite" => Duration.Inf
    case _          => underlying.getDuration(path).toScala
  }
}

object KafkaCluster {
  val GroupWhitelistDefault = List(".*")
  val GroupBlacklistDefault = List.empty[String]
  val TopicWhitelistDefault = List(".*")
  val TopicBlacklistDefault = List.empty[String]
}

final case class KafkaCluster(
    name: String,
    bootstrapBrokers: String,
    groupWhitelist: List[String] = KafkaCluster.GroupWhitelistDefault,
    groupBlacklist: List[String] = KafkaCluster.GroupBlacklistDefault,
    topicWhitelist: List[String] = KafkaCluster.TopicWhitelistDefault,
    topicBlacklist: List[String] = KafkaCluster.TopicBlacklistDefault,
    consumerProperties: Map[String, AnyRef] = Map.empty,
    adminClientProperties: Map[String, AnyRef] = Map.empty,
    labels: Map[String, String] = Map.empty
) {
  override def toString(): String = {
    s"""
       |  Cluster name: $name
       |  Cluster Kafka bootstrap brokers: $bootstrapBrokers
       |  Consumer group whitelist: [${groupWhitelist.mkString(", ")}]
       |  Consumer group blacklist: [${groupBlacklist.mkString(", ")}]
       |  Topic whitelist: [${topicWhitelist.mkString(", ")}]
       |  Topic blacklist: [${topicBlacklist.mkString(", ")}]
     """.stripMargin
  }
}

final case class ConduktorWatcherConfig(
    adminApiUrl: Uri,
    machineToMachine: MachineToMachine,
    organizationId: OrganizationId = OrganizationId(1)
)

object ConduktorWatcherConfig {
  final case class MachineToMachine(secret: Secret, issuer: Uri)
}

final case class AppConfig(
    pollInterval: FiniteDuration,
    lookupTable: LookupTableConfig,
    sinkConfigs: List[SinkConfig],
    clientGroupId: String,
    clientTimeout: FiniteDuration,
    retries: Int,
    clusters: List[KafkaCluster],
    strimziWatcher: Boolean,
    conduktorWatcher: SubConfiguration[ConduktorWatcherConfig]
) {
  override def toString(): String = {
    val clusterString =
      if (clusters.isEmpty)
        "  (none)"
      else clusters.map(_.toString).mkString("\n")
    val sinksString = sinkConfigs.mkString("")
    val lookupTableString = lookupTable.toString()
    s"""
       |Poll interval: $pollInterval
       |$lookupTableString
       |Metrics whitelist: [${sinkConfigs.head.metricWhitelist.mkString(", ")}]
       |Admin client consumer group id: $clientGroupId
       |Kafka client timeout: $clientTimeout
       |Kafka retries: $retries
       |$sinksString
       |Statically defined Clusters:
       |$clusterString
       |Watchers:
       |  Strimzi: $strimziWatcher
     """.stripMargin
  }

  def clustersGlobalLabels(): ClusterGlobalLabels = {
    clusters.map { cluster =>
      cluster.name -> cluster.labels
    }.toMap
  }
}
