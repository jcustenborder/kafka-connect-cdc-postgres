package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLClusterHealthCheck;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSQLConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSQLClusterHealthCheck.class)
public class PostgreSQLSourceTaskTest extends PostgreSQLTest {
  PostgreSQLSourceTask task;

  @BeforeEach
  public void start(@DockerFormatString(container = PostgreSQLConstants.CONTAINER_NAME, port = PostgreSQLConstants.PORT, format = PostgreSQLConstants.JDBC_URL_FORMAT) String jdbcUrl) {
    Map<String, String> settings = ImmutableMap.of(
        PostgreSQLSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        PostgreSQLSourceConnectorConfig.JDBC_PASSWORD_CONF, PostgreSQLConstants.PASSWORD,
        PostgreSQLSourceConnectorConfig.JDBC_USERNAME_CONF, PostgreSQLConstants.USERNAME,
        PostgreSQLSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAMES_CONF, "testing"
    );

    this.task = new PostgreSQLSourceTask();
    this.task.start(settings);
  }

  @Test
  public void foo() {

  }

  @AfterEach
  public void stop() {
    this.task.stop();
  }
}
