package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

public class PostgreSQLTestConstants {
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
    settings.put(PostgreSQLSourceConnectorConfig.SERVER_NAME_CONF, host);
    settings.put(PostgreSQLSourceConnectorConfig.SERVER_PORT_CONF, port.toString());
    settings.put(PostgreSQLSourceConnectorConfig.INITIAL_DATABASE_CONF, DATABASE_NAME);
    settings.put(PostgreSQLSourceConnectorConfig.JDBC_USERNAME_CONF, USERNAME);
    settings.put(PostgreSQLSourceConnectorConfig.JDBC_PASSWORD_CONF, PASSWORD);
    settings.put(PostgreSQLSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAME_CONF, REPLICATION_SLOT_NAME);
    return settings;
  }
}
