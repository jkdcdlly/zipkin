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
package com.twitter.zipkin.collector.filter

import com.twitter.ostrich.stats.Stats
import com.twitter.zipkin.collector.sampler.GlobalSampler
import com.twitter.zipkin.common.Span

class SamplerFilter(sampler: GlobalSampler) {
  def apply(spans: Seq[Span]): Seq[Span] = {
    spans.filter(span => {
      span.serviceNames.foreach { name => Stats.incr("received_" + name) }

      /**
       * If the span was created with debug mode on we guarantee that it will be
       * stored no matter what our sampler tells us
       */
      if (span.debug.getOrElse(false)) {
        Stats.incr("debugflag")
        (span.parentId, span.serviceName) match {
          case (None, Some(serviceName)) =>
            Stats.incr("debugflag_" + serviceName)
          case _ =>
        }
        true
      } else if (sampler(span.traceId)) {
        true
      } else {
        false
      }
    })
  }
}
