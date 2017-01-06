package io.confluent.kafka.connect.cdc.postgres;


import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLSettings;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLSettingsExtension;
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
@DockerCompose(dockerComposePath = PostgreSQLTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSQLClusterHealthCheck.class)
@ExtendWith(PostgreSQLSettingsExtension.class)
public class QueryServiceTest extends PostgreSQLTest {
  PostgreSQLSourceConnectorConfig config;
  QueryService queryService;
  TableMetadataProvider tableMetadataProvider;

  @BeforeEach
  public void settings(@PostgreSQLSettings Map<String, String> settings) {
    this.config = new PostgreSQLSourceConnectorConfig(settings);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new PostgreSQLTableMetadataProvider(this.config, offsetStorageReader);
    this.queryService = new QueryService(new SystemTime(), this.tableMetadataProvider, this.config);
  }

  @Test
  public void test() throws SQLException {
    this.queryService.query();
  }
}
