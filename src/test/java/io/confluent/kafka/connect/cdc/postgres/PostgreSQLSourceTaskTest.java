package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLClusterHealthCheck;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Disabled;

@Disabled
@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSQLTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSQLClusterHealthCheck.class)
public class PostgreSQLSourceTaskTest extends PostgreSQLTest {
//  PostgreSQLSourceTask task;
//
//  @BeforeEach
//  public void start(@DockerFormatString(container = PostgreSQLTestConstants.CONTAINER_NAME, port = PostgreSQLTestConstants.PORT, format = PostgreSQLTestConstants.JDBC_URL_FORMAT) String jdbcUrl) {
//    Map<String, String> settings = ImmutableMap.of(
//        PostgreSQLSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
//        PostgreSQLSourceConnectorConfig.JDBC_PASSWORD_CONF, PostgreSQLTestConstants.PASSWORD,
//        PostgreSQLSourceConnectorConfig.JDBC_USERNAME_CONF, PostgreSQLTestConstants.USERNAME,
//        PostgreSQLSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAME_CONF, "testing"
//    );
//
//    this.task = new PostgreSQLSourceTask();
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
