/*
 * Copyright 2013 Twitter Inc.
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

package com.twitter.zipkin.storage.anormdb

import com.twitter.finagle.stats.StatsReceiver
import java.sql.Connection

/**
 * Common database connection code.
 */
trait DBPool extends java.io.Closeable {

  val db: DB

  val stats: StatsReceiver

  // If an open database connection is supplied, it supersedes usage of the connection pool.
  val openCon: Option[Connection]

  /**
   * Closes all database connections.
   */
  override def close() = {
    if (!openCon.isEmpty) {
      openCon.get.close()
    }
    db.closeConnectionPool()
  }

  /**
   * Borrow a connection from the connection pool.
   *
   * @return a tuple containing the connection and a timestamp for stats tracking
   */
  def borrowConn(): (Connection, Long) = {
    val borrowTime = System.currentTimeMillis()
    if (openCon.isEmpty) {
      (db.getPooledConnection(), borrowTime)
    } else {
      (openCon.get, borrowTime)
    }
  }

  /**
   * Return a borrowed connection to the connection pool.
   */
  def returnConn(con: Connection, borrowTime: Long, method: String) = {
    if (openCon.isEmpty) {
      con.close()
    }
    stats.stat(method + "_msec").add(System.currentTimeMillis() - borrowTime)
  }
}
