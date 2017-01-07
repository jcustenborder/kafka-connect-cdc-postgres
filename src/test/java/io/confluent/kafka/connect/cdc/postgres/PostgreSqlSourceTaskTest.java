package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlClusterHealthCheck;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Disabled;

@Disabled
@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSqlClusterHealthCheck.class)
public class PostgreSqlSourceTaskTest extends PostgreSqlTest {
//  PostgreSqlSourceTask task;
//
//  @BeforeEach
//  public void start(@DockerFormatString(container = PostgreSqlTestConstants.CONTAINER_NAME, port = PostgreSqlTestConstants.PORT, format = PostgreSqlTestConstants.JDBC_URL_FORMAT) String jdbcUrl) {
//    Map<String, String> settings = ImmutableMap.of(
//        PostgreSqlSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
//        PostgreSqlSourceConnectorConfig.JDBC_PASSWORD_CONF, PostgreSqlTestConstants.PASSWORD,
//        PostgreSqlSourceConnectorConfig.JDBC_USERNAME_CONF, PostgreSqlTestConstants.USERNAME,
//        PostgreSqlSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAME_CONF, "testing"
//    );
//
//    this.task = new PostgreSqlSourceTask();
//    this.task.start(settings);
//  }
//
//  @Test
//  public void foo() {
//
//  }
//
//  @AfterEach
//  public void stop() {
//    this.task.stop();
//  }
}
