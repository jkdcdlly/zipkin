/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.sampler

import java.util.Random

import com.twitter.util.Var
import org.scalatest.FunSuite

class SamplerTest extends FunSuite {
  val rnd = new Random(1L)

  test("is permissive when the rate is 1") {
    val sampler = new Sampler(Var(1.0))
    assert(sampler(Long.MaxValue))
    assert(sampler(Long.MinValue))
    assert(sampler(rnd.nextLong()))
  }

  test("is exclusive when the rate is 0") {
    val sampler = new Sampler(Var(0.0))
    assert(!sampler(Long.MaxValue))
    assert(!sampler(Long.MinValue))
    assert(!sampler(rnd.nextLong()))
  }

  test("samples based on the given number") {
    val sampler = new Sampler(Var(0.5))
    assert(sampler(Long.MaxValue))
    assert(!sampler((Long.MaxValue * 0.5).toLong))
  }

  test("will update based on the given Var") {
    val v = Var(0.0)
    val sampler = new Sampler(v)
    assert(!sampler(Long.MaxValue))

    v.update(1.0)
    assert(sampler(Long.MaxValue))
  }
}
