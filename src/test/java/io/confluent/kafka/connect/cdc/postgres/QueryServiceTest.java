package io.confluent.kafka.connect.cdc.postgres;


import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlSettings;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlSettingsExtension;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.SQLException;
import java.util.Map;

import static org.mockito.Mockito.mock;

@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSqlClusterHealthCheck.class)
@ExtendWith(PostgreSqlSettingsExtension.class)
public class QueryServiceTest extends PostgreSqlTest {
  PostgreSqlSourceConnectorConfig config;
  QueryService queryService;
  TableMetadataProvider tableMetadataProvider;

  @BeforeEach
  public void settings(@PostgreSqlSettings Map<String, String> settings) {
    this.config = new PostgreSqlSourceConnectorConfig(settings);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new PostgreSqlTableMetadataProvider(this.config, offsetStorageReader);
    this.queryService = new QueryService(new SystemTime(), this.tableMetadataProvider, this.config);
  }

  @Test
  public void test() throws SQLException {
    this.queryService.query();
  }
}
