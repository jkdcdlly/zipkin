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
package com.twitter.zipkin.web

import com.twitter.finagle.tracing.SpanId
import com.twitter.zipkin.common.{Trace, Endpoint, Span, SpanTreeEntry}
import com.twitter.zipkin.web.Util.getRootMostSpan

case class SpanTimestamp(name: String, timestamp: Long, duration: Long) {
  def endTs = timestamp + duration
}

object TraceSummary {

  /**
   * Return a summary of this trace or none if we
   * cannot construct a trace summary. Could be that we have no spans.
   */
  def apply(trace: List[Span]): Option[TraceSummary] = {
    val duration = Trace.duration(trace).getOrElse(0L)
    val endpoints = trace.flatMap(_.endpoints).distinct
    for (
      traceId <- trace.headOption.map(_.traceId);
      timestamp <- trace.headOption.flatMap(_.timestamp)
    ) yield TraceSummary(
      SpanId(traceId).toString,
      timestamp,
      duration,
      spanTimestamps(trace),
      endpoints)
  }

  /**
   * Returns a map of services to a list of their durations
   */
  private def spanTimestamps(spans: List[Span]): List[SpanTimestamp] = {
    for {
      span <- spans.toList
      serviceName <- span.serviceNames
      timestamp <- span.timestamp
      duration <- span.duration
    } yield SpanTimestamp(serviceName, timestamp, duration)
  }

  /**
   * Figures out the "span depth". This is used in the ui
   * to figure out how to lay out the spans in the visualization.
   * @return span id -> depth in the tree
   */
  def toSpanDepths(spans: List[Span]): Map[Long, Int] = {
    getRootMostSpan(spans) match {
      case None => return Map.empty
      case Some(s) => {
        val spanTree = SpanTreeEntry.create(s, spans)
        spanTree.depths(1)
      }
    }
  }
}

/**
 * json-friendly representation of a trace summary
 *
 * @param traceId id of this trace
 * @param timestamp when did the trace start?
 * @param duration how long did the traced operation take?
 * @param endpoints endpoints involved in the traced operation
 */
case class TraceSummary(
  traceId: String,
  timestamp: Long,
  duration: Long,
  spanTimestamps: List[SpanTimestamp],
  endpoints: List[Endpoint])
