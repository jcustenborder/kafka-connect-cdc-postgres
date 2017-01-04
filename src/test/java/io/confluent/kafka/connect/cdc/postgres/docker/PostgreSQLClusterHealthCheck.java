package io.confluent.kafka.connect.cdc.postgres.docker;

import io.confluent.kafka.connect.cdc.docker.JdbcClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.PostgreSQLConstants;

public class PostgreSQLClusterHealthCheck extends JdbcClusterHealthCheck {
  public PostgreSQLClusterHealthCheck() {
    super(PostgreSQLConstants.CONTAINER_NAME,
        PostgreSQLConstants.PORT,
        PostgreSQLConstants.JDBC_URL_FORMAT,
        PostgreSQLConstants.USERNAME,
        PostgreSQLConstants.PASSWORD);
  }


}
