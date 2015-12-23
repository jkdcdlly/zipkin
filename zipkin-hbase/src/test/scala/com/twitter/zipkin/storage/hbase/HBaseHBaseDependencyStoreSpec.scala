package com.twitter.zipkin.storage.hbase

import com.twitter.util.{Await, Time}
import com.twitter.zipkin.common.{Dependencies, DependencyLink}
import com.twitter.zipkin.hbase.{DependencyStoreBuilder, TableLayouts}
import com.twitter.zipkin.storage.hbase.utils.HBaseTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

class HBaseHBaseDependencyStoreSpec extends ZipkinHBaseSpecification {

  val tablesNeeded = Seq(
    TableLayouts.dependenciesTableName,
    TableLayouts.idGenTableName,
    TableLayouts.mappingTableName
  )

  val dependencyStore = DependencyStoreBuilder(confOption = Some(_conf))()

  val d1 = DependencyLink("HBase.Client", "HBase.RegionServer", 1)
  val d2 = DependencyLink("HBase.Master", "HBase.RegionServer", 2)
  val deps = Dependencies(Time.fromSeconds(2).inMicroseconds, Time.fromSeconds(1000).inMicroseconds, List(d1, d2))

  test("storeDependencies") {
    Await.result(dependencyStore.storeDependencies(deps))
    val depsTable = new HBaseTable(_conf, TableLayouts.dependenciesTableName)
    val get = new Get(Bytes.toBytes(Long.MaxValue - deps.startTs * 1000))
    val result = Await.result(depsTable.get(Seq(get)))
    result.size should be(1)
  }

  test("getDependencies") {
    Await.result(dependencyStore.storeDependencies(deps))
    val retrieved = Await.result(dependencyStore.getDependencies(deps.endTs))
    retrieved should be(deps.links)
  }
}
