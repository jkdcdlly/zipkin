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

import com.twitter.zipkin.anormdb.SpanStoreBuilder
import com.twitter.zipkin.builder.QueryServiceBuilder
import com.twitter.zipkin.storage.anormdb.{AnormDependencyStore, AnormSpanStore, DB}

val serverPort = sys.env.get("QUERY_PORT").getOrElse("9411").toInt
val adminPort = sys.env.get("QUERY_ADMIN_PORT").getOrElse("9901").toInt
val logLevel = sys.env.get("QUERY_LOG_LEVEL").getOrElse("INFO")

val db = DB()

val spanStore = SpanStoreBuilder(db, true)()
val dependencies = AnormDependencyStore(db)

QueryServiceBuilder(
  "0.0.0.0:" + serverPort,
  adminPort,
  logLevel,
  spanStore,
  dependencies
)
