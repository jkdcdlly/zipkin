package com.twitter.zipkin.storage.redis

import com.google.common.io.Closer
import com.twitter.finagle.redis.Client
import com.twitter.util.{Duration, Future}
import com.twitter.zipkin.adjuster.ApplyTimestampAndDuration
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.storage._
import java.nio.ByteBuffer

/**
 * @param client the redis client to use
 * @param ttl expires keys older than this many seconds.
 */
class RedisSpanStore(client: Client, ttl: Option[Duration])
  extends SpanStore with CollectAnnotationQueries {
  private[this] val closer = Closer.create()
  private[this] val index = closer.register(new RedisIndex(client, ttl))
  private[this] val storage = closer.register(new RedisStorage(client, ttl))

  /** For testing, clear this store. */
  private[redis] def clear(): Future[Unit] = client.flushDB()

  override def close() = closer.close()

  override def apply(newSpans: Seq[Span]): Future[Unit] = Future.join(
    newSpans.map(s => s.copy(annotations = s.annotations.sorted))
            .map(ApplyTimestampAndDuration.apply)
            .flatMap(s => Seq(storage.storeSpan(s), index.index(s))))

  override def getTracesByIds(traceIds: Seq[Long]): Future[Seq[List[Span]]] = {
    storage.getSpansByTraceIds(traceIds)
  }

  override def getTraceIdsByName(
    serviceName: String,
    spanName: Option[String],
    endTs: Long,
    lookback: Long,
    limit: Int
  ): Future[Seq[IndexedTraceId]] = {
    index.getTraceIdsByName(serviceName, spanName, endTs * 1000, lookback * 1000, limit)
  }

  override def getTraceIdsByAnnotation(
    serviceName: String,
    annotation: String,
    value: Option[ByteBuffer],
    endTs: Long,
    lookback: Long,
    limit: Int
  ): Future[Seq[IndexedTraceId]] = {
    index.getTraceIdsByAnnotation(serviceName, annotation, value, endTs * 1000, lookback * 1000, limit)
  }

  override def getAllServiceNames() = index.getServiceNames.map(_.toList.sorted)

  override def getSpanNames(_serviceName: String) = {
    val serviceName = _serviceName.toLowerCase // service names are always lowercase!
    index.getSpanNames(serviceName).map(_.toList.sorted)
  }
}
