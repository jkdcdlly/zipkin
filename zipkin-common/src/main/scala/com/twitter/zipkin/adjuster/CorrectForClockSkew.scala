/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.twitter.zipkin.adjuster

import com.twitter.zipkin.Constants
import com.twitter.zipkin.common._

import scala.collection.{Map, breakOut}

/**
 * Adjusts Spans timestamps so that each child happens after their parents.
 * This is to counteract clock skew on servers, we want the Trace to happen in order.
 */
object CorrectForClockSkew extends ((List[Span]) => List[Span]) {

  override def apply(spans: List[Span]): List[Span] = {
    spans.find(!_.parentId.isDefined) match {
      case Some(s) => {
        val tree = SpanTreeEntry.create(s, spans)
        adjust(tree, None).toList
      }
      case None => spans
    }
  }

  /**
   * Recursively adjust the timestamps on the span tree. Root span is the reference point,
   * all children's timestamps gets adjusted based on that span's timestamps.
   */
  private[this] def adjust(span: SpanTreeEntry, previousSkew: Option[ClockSkew]): SpanTreeEntry = {

    val previousAdjustedSpan = previousSkew match {
      // adjust skew for particular endpoint brought over from the parent span
      case Some(endpointSkew) => adjustTimestamps(span, endpointSkew)
      case None => span
    }

    val validatedSpan = validateSpan(previousAdjustedSpan)

    // find out if the server endpoint has any skew compared to the client
    getClockSkew(validatedSpan.span) match {
      case Some(es) =>
        val adjustedSpan = adjustTimestamps(validatedSpan, es)
        // adjusting the timestamp of the endpoint with the skew
        // both for the current span and the direct children
        new SpanTreeEntry(adjustedSpan.span, adjustedSpan.children.map(adjust(_, Some(es))))

      case None => // could not figure out clock skew, return untouched.
        new SpanTreeEntry(validatedSpan.span, validatedSpan.children.map(adjust(_, None)))
    }
  }

  /**
   * Misc fixes to make sure the clock skew is adjusted correctly:
   * - For spans that only have CLIENT_SEND and CLIENT_RECEIVE annotations,
   *   create the corresponding SERVER_ annotations and propagate the skew
   *   adjustment to the children. TimeSkewAdjuster assumes that for a
   *   particular service, there is no skew between the server and client
   *   annotations; when the server annotations are missing, they are
   *   essentially estranged and we can't adjust the children accordingly.
   *   The created SERVER_ annotations have the same timestamp as the CLIENT_
   *   annotations.
   *   NOTE: this could also be refactored out into its own adjuster that would
   *   run before TimeSkewAdjuster, if necessary
   */
  private[this] def validateSpan(spanTree: SpanTreeEntry): SpanTreeEntry = {

    // if we have only CS and CR, inject a SR and SS
    val span = spanTree.span
    var annotations = span.annotations
    val annotationsMap = asMap(annotations)

    var children = spanTree.children
    if (containsCoreAnnotation(span.annotations) &&
        spanTree.children.length > 0 &&
        containsClientCoreAnnotations(annotationsMap) &&
        !containsServerCoreAnnotations(annotationsMap)
    ) {
      // pick up the endpoint first child's client send, if exists
      val endpoint = spanTree.children.head.span.clientSideAnnotations match {
        case head :: _ => head.host
        case _ => None
      }

      val serverRecvTs = span.annotations.find(_.value == Constants.ClientSend) match {
        case Some(a) =>
          annotations = annotations :+ Annotation(a.timestamp, Constants.ServerRecv, endpoint)
          a.timestamp
        case _ => // This should never actually happen since we checked in the IF
          throw new IllegalStateException("could not find cs in " + span)
      }

      val serverSendTs = span.annotations.find(_.value == Constants.ClientRecv) match {
        case Some(a) =>
          annotations = annotations :+ Annotation(a.timestamp, Constants.ServerSend, endpoint)
          a.timestamp
        case _ => // This should never actually happen since we checked in the IF
          throw new IllegalStateException("could not find cr in " + span)
      }

      /**
       * We need to manually propagate the clock skew for the children since
       * the assumption there is no clock skew on the same endpoint is no longer
       * valid if the children have clock skew.
       *
       * Since we're computing the skew in a particular endpoint, we use the
       * span's SERVER_* annotations as the client arguments and the
       * child's CLIENT_* annotations as the server arguments
       */
      children = children map { c =>
        asMap(c.span.annotations) match {
          case csa if containsClientCoreAnnotations(csa) =>
            val clientSendTs = csa(Constants.ClientSend).timestamp
            val clientRecvTs = csa(Constants.ClientRecv).timestamp
            endpoint flatMap { getClockSkew(serverRecvTs, serverSendTs, clientSendTs, clientRecvTs, _) } match {
              case Some(endpointSkew) => adjustTimestamps(c, endpointSkew)
              case _ => c
            }
          case _ =>
            // doesn't have all client core annotations, so don't do anything
            c
        }
      }
    }
    new SpanTreeEntry(span.copy(annotations = annotations), children)
  }

  /**
   * Do we have all the annotations we need to calculate clock skew?
   */
  private[this] def containsAllCoreAnnotations(annotations: Map[String, Annotation]): Boolean = {
    containsClientCoreAnnotations(annotations) && containsServerCoreAnnotations(annotations)
  }

  private[this] def containsClientCoreAnnotations(annotations: Map[String, Annotation]): Boolean = {
    annotations.contains(Constants.ClientSend) && annotations.contains(Constants.ClientRecv)
  }

  private[this] def containsServerCoreAnnotations(annotations: Map[String, Annotation]): Boolean = {
    annotations.contains(Constants.ServerSend) && annotations.contains(Constants.ServerRecv)
  }

  /**
   * Get the endpoint out of the first matching annotation.
   */
  private[this] def getEndpoint(
    allAnnotations: Map[String, Annotation],
    findByAnnotations: List[String]
  ): Option[Endpoint] = {
    findByAnnotations.map(allAnnotations.get(_).flatMap(_.host)).head
  }

  case class ClockSkew(endpoint: Endpoint, skew: Long)

  /**
   * Calculate the clock skew between two servers based
   * on the annotations in a span.
   */
  private[this] def getClockSkew(span: Span): Option[ClockSkew] = {
    val annotations = asMap(span.annotations)
    if (!containsAllCoreAnnotations(annotations)) None else {
      getEndpoint(annotations, List(Constants.ServerRecv, Constants.ServerSend)) flatMap { ep =>
        getClockSkew(
          getTimestamp(annotations, Constants.ClientSend),
          getTimestamp(annotations, Constants.ClientRecv),
          getTimestamp(annotations, Constants.ServerRecv),
          getTimestamp(annotations, Constants.ServerSend),
          ep
        )
      }
    }
  }

  /**
   * Calculate the clock skew between two servers based on annotations in a span
   *
   * Only adjust for clock skew if the core annotations are not in the following order:
   *  - Client send
   *  - Server receive
   *  - Server send
   *  - Client receive
   *
   * Special case: if the server (child) span is longer than the client (parent), then do not
   * adjust for clock skew.
   */
  private[this] def getClockSkew(
    clientSend: Long,
    clientRecv: Long,
    serverRecv: Long,
    serverSend: Long,
    endpoint: Endpoint
  ): Option[ClockSkew] = {
    val clientDuration = clientRecv - clientSend
    val serverDuration = serverSend - serverRecv

    // There is only clock skew if CS is after SR or CR is before SS
    val csAhead = clientSend < serverRecv
    val crAhead = clientRecv > serverSend
    if (serverDuration > clientDuration || (csAhead && crAhead)) None else {
      val latency = (clientDuration - serverDuration) / 2
      (serverRecv - latency - clientSend) match {
        case 0 => None
        case _ => Some(ClockSkew(endpoint, serverRecv - latency - clientSend))
      }
    }
  }

  /**
   * Extract timestamp for this particular event value.
   */
  private[this] def getTimestamp(annotations: Map[String, Annotation], value: String): Long = {
    annotations.get(value) match {
      case Some(a) => a.timestamp
      case None => throw new IncompleteTraceDataException("Could not find annotation matching " + value)
    }
  }

  /**
   * Adjust the span's annotation timestamps for the endpoint by skew ms.
   */
  private[this] def adjustTimestamps(spanTree: SpanTreeEntry, clockSkew: ClockSkew): SpanTreeEntry = {
    if (clockSkew.skew == 0) spanTree else {
      def isHost(ep: Endpoint, value: String): Boolean =
        clockSkew.endpoint.ipv4 == ep.ipv4 ||
        (value == Constants.ClientRecv || value == Constants.ClientSend) &&
        Constants.LocalhostLoopBackIP == ep.ipv4

      val span = spanTree.span
      val annotations: List[Annotation] = span.annotations.map(a => a.host match {
          case Some(ep) if isHost(ep, a.value) => a.copy(timestamp = a.timestamp - clockSkew.skew)
          case _ => a
        }
      ).sorted

      // reset timestamp and duration as if there's skew, these will change.
      val firstOption = annotations.headOption.map(_.timestamp)
      val lastOption = annotations.lastOption.map(_.timestamp)
      val duration = for (first <- firstOption; last <- lastOption; if (first != last))
        yield last - first
      new SpanTreeEntry(span.copy(
        timestamp = firstOption,
        duration = duration,
        annotations = annotations.sorted), spanTree.children)
    }
  }

  /**
   * @return true  if Span contains at most one of each core annotation
   *         false otherwise
   */
  private[adjuster] def containsCoreAnnotation(annotations: List[Annotation]): Boolean = {
    Constants.CoreAnnotations.map { c =>
      annotations.filter(_.value == c).length > 1
    }.count(b => b) == 0
  }

  /**
   * Get the annotations as a map with value to annotation bindings.
   */
  private[adjuster] def asMap(annotations: List[Annotation]): Map[String, Annotation] =
    annotations.map(a => a.value -> a)(breakOut)
}
