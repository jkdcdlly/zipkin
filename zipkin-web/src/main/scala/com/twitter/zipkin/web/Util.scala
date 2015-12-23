/*
 * Copyright 2014 Twitter Inc.
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

import java.util.Locale

import com.twitter.util.Duration
import com.twitter.zipkin.Constants.CoreAnnotationNames
import java.util.concurrent.TimeUnit
import com.twitter.zipkin.common.Span

object Util {
  private[this] val TimeUnits = Seq(
    TimeUnit.DAYS,
    TimeUnit.HOURS,
    TimeUnit.MINUTES,
    TimeUnit.SECONDS,
    TimeUnit.MILLISECONDS,
    TimeUnit.MICROSECONDS)

  private[this] val TimeUnitNames = Map(
    TimeUnit.DAYS -> ("days", "day"),
    TimeUnit.HOURS -> ("hrs", "hr"),
    TimeUnit.MINUTES -> ("min", "min"))

  def durationStr(duration: Long): String = durationStr(Duration.fromMicroseconds(duration))

  def durationStr(d: Duration): String = {
    var micros = d.inMicroseconds
    TimeUnits.dropWhile(_.convert(micros, TimeUnit.MICROSECONDS) == 0) match {
      case s if s.size == 0 => ""

      // seconds
      case Seq(_, _, _) => "%.3fs".formatLocal(Locale.ENGLISH, d.inMilliseconds / 1000.0)

      // milliseconds
      case Seq(_, _) => "%.3fms".formatLocal(Locale.ENGLISH, d.inMicroseconds / 1000.0)

      // microseconds
      case Seq(_) => "%dμ".formatLocal(Locale.ENGLISH, d.inMicroseconds)

      // seconds or more
      case _ =>
        val s = new StringBuilder
        for ((u, (pName, sName)) <- TimeUnitNames) {
          val v = u.convert(micros, TimeUnit.MICROSECONDS)
          if (v != 0) {
            micros -= TimeUnit.MICROSECONDS.convert(v, u)
            if (v > 0 && !s.isEmpty)
              s.append(" ")
            s.append(v.toString)
            s.append(if (v == 0 || v > 1) pName else sName)
          }
        }

        if (micros > 0) {
          if (!s.isEmpty)
            s.append(" ")
          s.append(durationStr(micros))
        }

        s.toString()
    }
  }

  def annoToString(value: String): String =
    CoreAnnotationNames.get(value).getOrElse(value)

  def getRootSpans(spans: List[Span]): List[Span] = {
    val idSpan = getIdToSpanMap(spans)
    spans filter { !_.parentId.flatMap(idSpan.get).isDefined }
  }

  /**
   * In some cases we don't care if it's the actual root span or just the span
   * that is closes to the root. For example it could be that we don't yet log spans
   * from the root service, then we want the one just below that.
   * FIXME if there are holes in the trace this might not return the correct span
   */
  private[web] def getRootMostSpan(spans: List[Span]): Option[Span] = {
    spans.find(!_.parentId.isDefined) orElse {
      val idSpan = getIdToSpanMap(spans)
      spans.headOption map {
        recursiveGetRootMostSpan(idSpan, _)
      }
    }
  }

  private def recursiveGetRootMostSpan(idSpan: Map[Long, Span], prevSpan: Span): Span = {
    // parent id shouldn't be none as then we would have returned already
    val span = for (id <- prevSpan.parentId; s <- idSpan.get(id)) yield
    recursiveGetRootMostSpan(idSpan, s)
    span.getOrElse(prevSpan)
  }

  /*
   * Turn the Trace into a map of Span Id -> Span
   */
  def getIdToSpanMap(spans: List[Span]): Map[Long, Span] = spans.map(s => (s.id, s)).toMap
}
