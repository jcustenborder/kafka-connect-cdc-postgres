/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.JdbcUtils;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class QueryService extends AbstractExecutionThreadService {
  private static final Logger log = LoggerFactory.getLogger(QueryService.class);

  final Time time;
  final TableMetadataProvider tableMetadataProvider;
  final PostgreSqlSourceConnectorConfig config;
  final RateLimiter queryRateLimiter;
  final PostgreSqlChange.Builder changeBuilder;
  final ChangeWriter changeWriter;

  QueryService(Time time, TableMetadataProvider tableMetadataProvider, PostgreSqlSourceConnectorConfig config, ChangeWriter changeWriter) {
    this.time = time;
    this.tableMetadataProvider = tableMetadataProvider;
    this.config = config;
    this.changeWriter = changeWriter;
    this.queryRateLimiter = RateLimiter.create(10);
    this.changeBuilder = new PostgreSqlChange.Builder(this.config, this.time, this.tableMetadataProvider);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {
        this.queryRateLimiter.acquire();
        query();
      } catch (Exception ex) {
        log.error("Exception thrown", ex);
      }
    }
  }

  void query() throws SQLException {
    PooledConnection pooledConnection = null;
    try {
      pooledConnection = JdbcUtils.openPooledConnection(this.config, null);
      final String sql = "SELECT * FROM pg_logical_slot_get_changes(?, ?, ?, 'skip-empty-xacts', '1', 'force-binary', '0', 'include-timestamp', '1', 'include-xids', '1')";
      try (PreparedStatement statement = pooledConnection.getConnection().prepareStatement(sql)) {
        statement.setString(1, this.config.replicationSlotName);
        statement.setObject(2, null);
        statement.setInt(3, 1024); //Number of changes to stream

        try (ResultSet results = statement.executeQuery()) {
          while (results.next()) {
            PostgreSqlChange change = this.changeBuilder.build(results);
            this.changeWriter.addChange(change);
          }
        }
      }
    } finally {
      JdbcUtils.closeConnection(pooledConnection);
    }
  }
}
