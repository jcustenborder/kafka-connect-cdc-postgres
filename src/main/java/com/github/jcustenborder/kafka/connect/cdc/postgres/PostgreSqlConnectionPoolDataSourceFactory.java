/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.google.common.base.Strings;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.ConnectionKey;
import com.github.jcustenborder.kafka.connect.cdc.ConnectionPoolDataSourceFactory;
import org.postgresql.ds.PGConnectionPoolDataSource;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

class PostgreSqlConnectionPoolDataSourceFactory implements ConnectionPoolDataSourceFactory {

  final PostgreSqlSourceConnectorConfig config;

  PostgreSqlConnectionPoolDataSourceFactory(PostgreSqlSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException {
    PGConnectionPoolDataSource poolingDataSource = new PGConnectionPoolDataSource();
    poolingDataSource.setServerName(this.config.serverName);
    poolingDataSource.setPortNumber(this.config.serverPort);
    poolingDataSource.setUser(this.config.jdbcUsername);
    poolingDataSource.setPassword(this.config.jdbcPassword);

    if (Strings.isNullOrEmpty(connectionKey.databaseName)) {
      poolingDataSource.setDatabaseName(this.config.initialDatabase);
    } else {
      poolingDataSource.setDatabaseName(connectionKey.databaseName);
    }
    return poolingDataSource;
  }

  @Override
  public ConnectionKey connectionKey(ChangeKey changeKey) {
    return ConnectionKey.of(this.config.serverName, this.config.serverPort, this.config.jdbcUsername, changeKey.databaseName);
  }
}
