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

import com.github.jcustenborder.kafka.connect.cdc.ConnectionPoolDataSourceFactory;
import com.github.jcustenborder.kafka.connect.cdc.PooledCDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class PostgreSqlSourceConnectorConfig extends PooledCDCSourceConnectorConfig {

  public static final String POSTGRES_REPLICATION_SLOT_NAME_CONF = "postgres.replication.slot.name";
  static final String POSTGRES_REPLICATION_SLOT_NAME_DOC = "THe replication slot names to connect to.";

  public final String replicationSlotName;
  final PostgreSqlConnectionPoolDataSourceFactory dataSourceFactory;

  public PostgreSqlSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);
    this.replicationSlotName = this.getString(POSTGRES_REPLICATION_SLOT_NAME_CONF);
    this.dataSourceFactory = new PostgreSqlConnectionPoolDataSourceFactory(this);
  }

  public static ConfigDef config() {
    return PooledCDCSourceConnectorConfig.config()
        .define(POSTGRES_REPLICATION_SLOT_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, POSTGRES_REPLICATION_SLOT_NAME_DOC);
  }

  @Override
  public ConnectionPoolDataSourceFactory connectionPoolDataSourceFactory() {
    return this.dataSourceFactory;
  }
}
