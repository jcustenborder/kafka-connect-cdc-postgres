package io.confluent.kafka.connect.cdc.postgres;

public class PostgreSQLConstants {
  public static final String CONTAINER_NAME = "postgres";
  public static final int PORT = 5432;
  public static final String USERNAME = "postgres";
  public static final String PASSWORD = "password";
  public static final String JDBC_URL_FORMAT = "jdbc:postgresql://$HOST:$EXTERNAL_PORT/CDC_TESTING";
  public static final String DOCKER_COMPOSE_FILE = "src/test/resources/docker-compose.yml";
}
