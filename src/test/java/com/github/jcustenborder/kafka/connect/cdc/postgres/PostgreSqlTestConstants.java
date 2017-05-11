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

import com.github.jcustenborder.kafka.connect.cdc.postgres.PostgreSqlSourceConnectorConfig;
import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

public class PostgreSqlTestConstants {
  public static final String CONTAINER_NAME = "postgres";
  public static final int PORT = 5432;
  public static final String USERNAME = "postgres";
  public static final String PASSWORD = "password";
  public static final String DATABASE_NAME = "CDC_TESTING";
  public static final String JDBC_URL_FORMAT = "jdbc:postgresql://$HOST:$EXTERNAL_PORT/CDC_TESTING";
  public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose.yml";
  public static final String REPLICATION_SLOT_NAME = "kafka";

  public static Map<String, String> settings(String host, Integer port) {
    Preconditions.checkNotNull(host, "host cannot be null");
    Preconditions.checkNotNull(port, "port cannot be null");
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(PostgreSqlSourceConnectorConfig.SERVER_NAME_CONF, host);
    settings.put(PostgreSqlSourceConnectorConfig.SERVER_PORT_CONF, port.toString());
    settings.put(PostgreSqlSourceConnectorConfig.INITIAL_DATABASE_CONF, DATABASE_NAME);
    settings.put(PostgreSqlSourceConnectorConfig.JDBC_USERNAME_CONF, USERNAME);
    settings.put(PostgreSqlSourceConnectorConfig.JDBC_PASSWORD_CONF, PASSWORD);
    settings.put(PostgreSqlSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAME_CONF, REPLICATION_SLOT_NAME);
    return settings;
  }
}
