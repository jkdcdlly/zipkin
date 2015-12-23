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
package com.twitter.zipkin.storage.cassandra

import java.nio.ByteBuffer
import com.twitter.scrooge.{ThriftStructCodec, BinaryThriftStructSerializer, ThriftStruct}

class ScroogeThriftCodec[T <: ThriftStruct](structCodec: ThriftStructCodec[T]) extends Codec[T] {
  val serializer = new BinaryThriftStructSerializer[T] { def codec = structCodec }

  def encode(t: T): ByteBuffer = b2b(serializer.toBytes(t))

  def decode(ary: ByteBuffer): T = serializer.fromBytes(b2b(ary))
}
