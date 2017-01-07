package io.confluent.kafka.connect.cdc.postgres;


import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlSettings;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSqlSettingsExtension;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;

@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSqlClusterHealthCheck.class)
@ExtendWith(PostgreSqlSettingsExtension.class)
public class PostgreSqlTableMetadataProviderTest extends PostgreSqlTest {
  private static final Logger log = LoggerFactory.getLogger(PostgreSqlTableMetadataProviderTest.class);
  PostgreSqlSourceConnectorConfig config;
  PostgreSqlTableMetadataProvider tableMetadataProvider;

  @BeforeEach
  public void settings(@PostgreSqlSettings Map<String, String> settings) {
    this.config = new PostgreSqlSourceConnectorConfig(settings);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new PostgreSqlTableMetadataProvider(this.config, offsetStorageReader);
  }

  @TestFactory
  public Stream<DynamicTest> fetchTableMetadata(
      @DockerFormatString(container = PostgreSqlTestConstants.CONTAINER_NAME, port = PostgreSqlTestConstants.PORT, format = PostgreSqlTestConstants.JDBC_URL_FORMAT) String jdbcUrl
  ) throws SQLException {

    List<ChangeKey> tables = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSqlTestConstants.USERNAME, PostgreSqlTestConstants.PASSWORD)) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("SELECT table_catalog, table_schema, table_name from information_schema.tables where lower(table_schema) = lower('public')")) {
          while (resultSet.next()) {
            String tableCatalog = resultSet.getString(1);
            String tableSchema = resultSet.getString(2);
            String tableName = resultSet.getString(3);
            ChangeKey changeKey = new ChangeKey(tableCatalog, tableSchema, tableName);
            tables.add(changeKey);
          }
        }
      }
    }


    return tables.stream().map(data -> dynamicTest(data.tableName, () -> fetchTableMetadata(jdbcUrl, data)));
  }

  void fetchTableMetadata(String jdbcUrl, ChangeKey changeKey) throws SQLException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSqlTestConstants.USERNAME, PostgreSqlTestConstants.PASSWORD)) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(
            String.format("Select * from %s limit 1", changeKey.tableName))) {//
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              String columnName = resultSetMetaData.getColumnName(i);
              String columnType = resultSetMetaData.getColumnClassName(i);
              Object value = resultSet.getObject(i);
              log.trace("{}:{} = {} = {}", changeKey.tableName, columnName, columnType, value);
            }
          }
        }
      }
    }

    TableMetadataProvider.TableMetadata tableMetadata = this.tableMetadataProvider.fetchTableMetadata(changeKey);


  }
}
