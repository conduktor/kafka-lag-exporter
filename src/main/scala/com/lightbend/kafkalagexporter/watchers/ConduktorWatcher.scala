/*
 * Copyright (C) 2018-2022 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2022 Sean Glover <https://seanglover.com>
 */

package com.lightbend.kafkalagexporter.watchers

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.lightbend.kafkalagexporter.{ConduktorWatcherConfig, KafkaCluster, KafkaClusterManager}
import eu.timepit.refined
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal.JsonStringContext
import io.conduktor.api.common.dtos.AuthToken
import io.conduktor.api.server.clusters.Certificate.EncodedCertificate
import io.conduktor.api.server.clusters.values.SharedClusterProperties
import io.conduktor.api.server.clusters.{SharedClusterResponseV2, endpoints}
import io.conduktor.certificate.{KafkaConnectionProperties, PemCertificate}
import io.conduktor.primitives.types.Secret
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}
import sttp.client3.SttpBackend
import sttp.client3.akkahttp.AkkaHttpBackend
import sttp.tapir.client.sttp.SttpClientInterpreter

import java.io.StringReader
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.{Try, Using}

class ConduktorClient(config: ConduktorWatcherConfig)(implicit
    nonBlockingIOEc: ExecutionContext
) {

  final case class ClaimsPayload(
      sourceApplication: String,
      userMail: Option[String]
  )

  val token: AuthToken = {
    import config.machineToMachine._
    ClaimsPayload(sourceApplication = "monitoring", userMail = None)
    refined
      .refineV[NonEmpty](
        JwtCirce.encode(
          JwtClaim(
            content = json"""{ "sourceApplication": "monitoring" }""".spaces2,
            issuer = Some(issuer.toString())
          ),
          Secret.unwrapValue(secret),
          JwtAlgorithm.HS256
        )
      )
      .fold(error => throw new IllegalArgumentException(error), AuthToken.apply)
  }

  val http: SttpBackend[Future, Any] = AkkaHttpBackend()

  def listClusters(
      certificates: List[EncodedCertificate]
  ): Future[List[KafkaCluster]] =
    SttpClientInterpreter()
      .toSecureClientThrowErrors(
        endpoints.v2.getClustersM2m,
        Some(config.adminApiUrl),
        http
      )
      .apply(token)
      .apply(config.organizationId)
      .flatMap { clusters =>
        Future.traverse(clusters)(mapToKafkaCluster(certificates))
      }

  def start(clusterWatcher: Watcher.Events): Watcher.Client = {
    (for {
      certificates <- listCertificates
      clusters <- listClusters(certificates)
    } yield {
      clusters.foreach(clusterWatcher.added)
    })
      .recover { t => clusterWatcher.error(t) }
    val client: Watcher.Client = () => ()
    client
  }

  private def mapToKafkaCluster(
      certificates: List[EncodedCertificate]
  )(cluster: SharedClusterResponseV2) =
    Future.fromTry(
      parseToMap(cluster.properties)
        .map(_ ++ buildPropertiesForCertificates(certificates, cluster))
        .map(properties =>
          KafkaCluster(
            name = cluster.id.value.toString,
            bootstrapBrokers = cluster.bootstrapServers.value.value,
            groupWhitelist = KafkaCluster.GroupWhitelistDefault,
            groupBlacklist = KafkaCluster.GroupBlacklistDefault,
            topicWhitelist = KafkaCluster.TopicWhitelistDefault,
            topicBlacklist = KafkaCluster.TopicBlacklistDefault,
            consumerProperties = properties,
            adminClientProperties = properties,
            labels = Map.empty
          )
        )
    )

  def listCertificates: Future[List[EncodedCertificate]] =
    SttpClientInterpreter()
      .toSecureClientThrowErrors(
        endpoints.certificate.listEncodedCertificates,
        Some(config.adminApiUrl),
        http
      )
      .apply(token)
      .apply(config.organizationId)

  private def buildPropertiesForCertificates(certificates: List[EncodedCertificate], cluster: SharedClusterResponseV2) = {
    val serverCertCheck = if (cluster.ignoreUntrustedCertificate) {
      KafkaConnectionProperties.ServerCertificateCheck.IgnoreUntrustedCertificate
    } else {
      val pemCertificates = certificates.map(encodedCertificate => PemCertificate(encodedCertificate.encoded.encoded))
      KafkaConnectionProperties.ServerCertificateCheck.CheckChain(pemCertificates)
    }
    val clientCertCheck = cluster.accessCert.zip(cluster.accessKey)
      .map { case (cert, key) => KafkaConnectionProperties.ClientCertificateAuth.CertAuth(PemCertificate(cert.value.value), PemCertificate(key.value.value)) }
      .getOrElse(KafkaConnectionProperties.ClientCertificateAuth.NoAuth)

    KafkaConnectionProperties.forCertificates(serverCertCheck, clientCertCheck)
  }

  def parseToMap(
      properties: Option[SharedClusterProperties]
  ): Try[Map[String, String]] =
    properties
      .map(prop =>
        Using(new StringReader(prop.value)) { reader =>
          val properties = new java.util.Properties()
          properties.load(reader) // Will throw if something's wrong
          properties
            .entrySet()
            .asScala
            .map(entry => (entry.getKey.toString, entry.getValue.toString))
            .toMap
        }
      )
      .getOrElse(Try(Map.empty[String, String]))

}

object ConduktorWatcher {
  val name: String = "conduktor"

  def init(
      handler: ActorRef[KafkaClusterManager.Message],
      config: ConduktorWatcherConfig
  )(implicit nonBlockingIOEc: ExecutionContext): Behavior[Watcher.Message] =
    Behaviors.setup { context =>
      val watcher = new Watcher.Events {
        override def added(cluster: KafkaCluster): Unit =
          handler ! KafkaClusterManager.ClusterAdded(cluster)
        override def removed(cluster: KafkaCluster): Unit =
          handler ! KafkaClusterManager.ClusterRemoved(cluster)
        override def error(e: Throwable): Unit =
          context.log.error(e.getMessage, e)
      }
      val client = new ConduktorClient(config).start(watcher)
      watch(client)
    }

  def watch(client: Watcher.Client): Behaviors.Receive[Watcher.Message] =
    Behaviors.receive { case (context, _: Watcher.Stop) =>
      Behaviors.stopped { () =>
        client.close()
        context.log.info("Gracefully stopped StrimziKafkaWatcher")
      }
    }
}
