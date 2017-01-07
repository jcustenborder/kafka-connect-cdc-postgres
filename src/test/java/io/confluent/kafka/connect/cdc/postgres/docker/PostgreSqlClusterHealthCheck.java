package io.confluent.kafka.connect.cdc.postgres.docker;

import io.confluent.kafka.connect.cdc.docker.JdbcClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.PostgreSqlTestConstants;

public class PostgreSqlClusterHealthCheck extends JdbcClusterHealthCheck {
  public PostgreSqlClusterHealthCheck() {
    super(PostgreSqlTestConstants.CONTAINER_NAME,
        PostgreSqlTestConstants.PORT,
        PostgreSqlTestConstants.JDBC_URL_FORMAT,
        PostgreSqlTestConstants.USERNAME,
        PostgreSqlTestConstants.PASSWORD);
  }


}
