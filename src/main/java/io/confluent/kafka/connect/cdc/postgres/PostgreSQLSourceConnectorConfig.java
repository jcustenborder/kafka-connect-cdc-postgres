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

import io.confluent.kafka.connect.cdc.CDCSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class PostgreSQLSourceConnectorConfig extends CDCSourceConnectorConfig {

  public static final String JDBC_USERNAME_CONF = "jdbc.username";
  public static final String JDBC_PASSWORD_CONF = "jdbc.password";
  public static final String JDBC_URL_CONF = "jdbc.url";
  public static final String POSTGRES_REPLICATION_SLOT_NAMES_CONF = "postgres.replication.slot.names";
  static final String POSTGRES_REPLICATION_SLOT_NAMES_DOC = "THe replication slot names to connect to.";
  static final String JDBC_USERNAME_DOC = "JDBC Username to connect to Oracle with.";
  static final String JDBC_PASSWORD_DOC = "JDBC Password to connect to Oracle with.";
  static final String JDBC_URL_DOC = "JDBC Url to connect to oracle with. You should not inline your username and password.";

  public final String jdbcUrl;
  public final String jdbcUsername;
  public final String jdbcPassword;
  public final List<String> replicationSlotNames;

  public PostgreSQLSourceConnectorConfig(Map<String, String> parsedConfig) {
    super(config(), parsedConfig);

    this.jdbcUrl = this.getString(JDBC_URL_CONF);
    this.jdbcUsername = this.getString(JDBC_USERNAME_CONF);
    this.jdbcPassword = this.getPassword(JDBC_PASSWORD_CONF).value();
    this.replicationSlotNames = this.getList(POSTGRES_REPLICATION_SLOT_NAMES_CONF);

  }

  public static ConfigDef config() {
    return CDCSourceConnectorConfig.config()
        .define(JDBC_URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_URL_DOC)
        .define(JDBC_USERNAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, JDBC_USERNAME_DOC)
        .define(JDBC_PASSWORD_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, JDBC_PASSWORD_DOC)
        .define(POSTGRES_REPLICATION_SLOT_NAMES_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, POSTGRES_REPLICATION_SLOT_NAMES_DOC);
  }
}
