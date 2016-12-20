/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.JdbcCDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class PostgreSQLSourceConnectorConfig extends JdbcCDCSourceConnectorConfig {

  public static final String POSTGRES_REPLICATION_SLOT_NAMES_CONF = "postgres.replication.slot.names";
  static final String POSTGRES_REPLICATION_SLOT_NAMES_DOC = "THe replication slot names to connect to.";

  public final List<String> replicationSlotNames;

  public PostgreSQLSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);
    this.replicationSlotNames = this.getList(POSTGRES_REPLICATION_SLOT_NAMES_CONF);
  }

  public static ConfigDef config() {
    return JdbcCDCSourceConnectorConfig.config()
        .define(POSTGRES_REPLICATION_SLOT_NAMES_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, POSTGRES_REPLICATION_SLOT_NAMES_DOC);
  }
}
