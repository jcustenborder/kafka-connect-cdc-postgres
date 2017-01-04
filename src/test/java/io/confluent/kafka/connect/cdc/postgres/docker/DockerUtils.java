package io.confluent.kafka.connect.cdc.postgres.docker;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import io.confluent.kafka.connect.cdc.postgres.docker.healthcheck.PostgreSQLHealthCheck;

public class DockerUtils {
  public static final String JDBC_URL_FORMAT = "jdbc:postgresql://$HOST:$EXTERNAL_PORT/CDC_TESTING";// +
//      "jdbc:oracle:oci:@$HOST:$EXTERNAL_PORT";

  public static final String POSTGRES_CONTAINER = "postgres";
  public static final String USERNAME = "postgres";
  public static final String PASSWORD = "password";

  public static DockerComposeRule postgresql() {
    return DockerComposeRule.builder()
        .file("src/test/resources/docker-compose.yml")
        .waitingForService(DockerUtils.POSTGRES_CONTAINER, new PostgreSQLHealthCheck(USERNAME, PASSWORD))
        .saveLogsTo("target/postgresql")
        .build();
  }

  public static DockerPort postgreSQLPort(Container oracleContainer) {
    return oracleContainer.port(5432);
  }

  public static Container postgreSQL(DockerComposeRule docker) {
    return docker.containers().container(POSTGRES_CONTAINER);
  }

  public static String jdbcUrl(DockerComposeRule docker) {
    Container oracleContainer = postgreSQL(docker);
    DockerPort dockerPort = postgreSQLPort(oracleContainer);
    return jdbcUrl(dockerPort);
  }

  public static String jdbcUrl(Container oracleContainer) {
    DockerPort dockerPort = postgreSQLPort(oracleContainer);
    return jdbcUrl(dockerPort);
  }

  public static String jdbcUrl(DockerPort dockerPort) {
    return dockerPort.inFormat(JDBC_URL_FORMAT);
  }
}
