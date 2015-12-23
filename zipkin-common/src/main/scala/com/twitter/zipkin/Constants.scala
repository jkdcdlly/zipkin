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
package com.twitter.zipkin

import com.twitter.conversions.time._
import com.twitter.util.Duration

object Constants {
  val ClientSend: String = "cs"
  val ClientSendFragment: String = "csf"
  val ClientRecv: String = "cr"
  val ClientRecvFragment: String = "crf"
  val ServerSend: String = "ss"
  val ServerSendFragment: String = "ssf"
  val ServerRecv: String = "sr"
  val ServerRecvFragment: String = "srf"
  val ClientAddr: String = "ca"
  val ServerAddr: String = "sa"
  val WireSend: String = "ws"
  val WireRecv: String = "wr"
  val LocalComponent: String = "lc"

  val CoreClient: Set[String] = Set(ClientSend, ClientSendFragment, ClientRecv, ClientRecvFragment)
  val CoreServer: Set[String] = Set(ServerRecv, ServerRecvFragment, ServerSend, ServerSendFragment)
  val CoreAddress: Set[String] = Set(ClientAddr, ServerAddr)
  val CoreWire: Set[String] = Set(WireSend, WireRecv)
  val CoreLocal: Set[String] = Set(LocalComponent)

  val CoreAnnotations: Set[String] = CoreClient ++ CoreServer ++ CoreWire ++ CoreLocal

  val CoreAnnotationNames: Map[String, String] = Map(
    ClientSend -> "Client Send",
    ClientSendFragment -> "Client Send Fragment",
    ClientRecv -> "Client Receive",
    ClientRecvFragment -> "Client Receive Fragment",
    ServerSend -> "Server Send",
    ServerSendFragment -> "Server Send Fragment",
    ServerRecv -> "Server Receive",
    ServerRecvFragment -> "Server Receive Fragment",
    ClientAddr -> "Client Address",
    ServerAddr -> "Server Address",
    WireSend -> "Wire Send",
    WireRecv -> "Wire Receive",
    LocalComponent -> "Local Component"
  )

  /* 127.0.0.1 */
  val LocalhostLoopBackIP = (127 << 24) | 1

  /* Amount of time padding to use when resolving complex query timestamps */
  val TraceTimestampPadding: Duration = 1.minute

  /* Max number of services for which there will be no HTTP Cache-Control header returned when getting services */
  val MaxServicesWithoutCaching: Int = 3
}
