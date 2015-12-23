package com.twitter.zipkin.storage.hbase

import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.Future
import com.twitter.zipkin.common.{DependencyLink, Dependencies}
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.hbase.TableLayouts
import com.twitter.zipkin.storage.DependencyStore
import com.twitter.zipkin.storage.hbase.utils.HBaseTable
import com.twitter.zipkin.thriftscala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

class HBaseDependencyStore(conf: Configuration) extends DependencyStore {

  val dependenciesTable = new HBaseTable(conf, TableLayouts.dependenciesTableName)

  val serializer = new BinaryThriftStructSerializer[thriftscala.Dependencies] {
    def codec = thriftscala.Dependencies
  }

  def close() = dependenciesTable.close()

  override def getDependencies(endTs: Long, lookback: Option[Long] = None): Future[Seq[DependencyLink]] = {
    val scan = new Scan()
    val startTs = endTs - lookback.getOrElse(endTs)
    scan.setStartRow(Bytes.toBytes(startTs))
    lookback.foreach { ed => scan.setStopRow(Bytes.toBytes(Long.MaxValue - ed * 1000)) }
    scan.addColumn(TableLayouts.dependenciesFamily, Bytes.toBytes("\0"))
    dependenciesTable.scan(scan, 100).map { results =>
      results.flatMap { result =>
        result.list().asScala.headOption.map { kv =>
          val tDep = serializer.fromBytes(kv.getValue)
          tDep.toDependencies
        }
      }.foldLeft(Dependencies.zero)((l, r) => l + r)
    }.map(_.links)
  }

  override def storeDependencies(dependencies: Dependencies): Future[Unit] = {
    val rk = Bytes.toBytes(Long.MaxValue - dependencies.startTs * 1000)
    val put = new Put(rk)
    put.add(TableLayouts.dependenciesFamily, Bytes.toBytes("\0"), serializer.toBytes(dependencies.toThrift))
    dependenciesTable.put(Seq(put))
  }

}
