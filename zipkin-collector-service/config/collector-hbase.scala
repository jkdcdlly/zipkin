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

import com.twitter.zipkin.collector.builder.{Adjustable, CollectorServiceBuilder, ZipkinServerBuilder}
import com.twitter.zipkin.hbase
import com.twitter.zipkin.receiver.kafka.KafkaSpanReceiverFactory
import com.twitter.zipkin.storage.Store
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin

val zkServers = Some("zookeeper01");
val zkPort = Some(2181);

val storeBuilder = Store.Builder(
  hbase.SpanStoreBuilder(zkServers = zkServers, zkPort = zkPort),
  hbase.DependencyStoreBuilder(zkServers = zkServers, zkPort = zkPort)
)


//create HBase table
//val conf = HBaseConfiguration.create()
//val hbaseAdmin = new HBaseAdmin(conf)
//hbase.TableLayouts.createTables(hbaseAdmin)


val kafkaReceiver = sys.env.get("KAFKA_ZOOKEEPER").map(
  KafkaSpanReceiverFactory.factory(_, sys.env.get("KAFKA_TOPIC").getOrElse("zipkin"))
)

val serverPort = sys.env.get("COLLECTOR_PORT").getOrElse("9410").toInt
val adminPort = sys.env.get("COLLECTOR_ADMIN_PORT").getOrElse("9900").toInt
val logLevel = sys.env.get("COLLECTOR_LOG_LEVEL").getOrElse("INFO")
val sampleRate = sys.env.get("COLLECTOR_SAMPLE_RATE").getOrElse("1.0").toDouble
CollectorServiceBuilder(
  storeBuilder,
  kafkaReceiver,
  serverBuilder = ZipkinServerBuilder(serverPort, adminPort),
  logLevel = logLevel
).sampleRate(Adjustable.local(sampleRate))
