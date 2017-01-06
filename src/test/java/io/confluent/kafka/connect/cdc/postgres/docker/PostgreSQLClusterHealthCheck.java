package io.confluent.kafka.connect.cdc.postgres.docker;

import io.confluent.kafka.connect.cdc.docker.JdbcClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.PostgreSQLTestConstants;

public class PostgreSQLClusterHealthCheck extends JdbcClusterHealthCheck {
  public PostgreSQLClusterHealthCheck() {
    super(PostgreSQLTestConstants.CONTAINER_NAME,
        PostgreSQLTestConstants.PORT,
        PostgreSQLTestConstants.JDBC_URL_FORMAT,
        PostgreSQLTestConstants.USERNAME,
        PostgreSQLTestConstants.PASSWORD);
  }


}
