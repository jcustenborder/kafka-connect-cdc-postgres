package io.confluent.kafka.connect.cdc.postgres;


import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.Integration;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.docker.DockerCompose;
import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLClusterHealthCheck;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLSettings;
import io.confluent.kafka.connect.cdc.postgres.docker.PostgreSQLSettingsExtension;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
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
@DockerCompose(dockerComposePath = PostgreSQLTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSQLClusterHealthCheck.class)
@ExtendWith(PostgreSQLSettingsExtension.class)
public class PostgreSQLTableMetadataProviderTest extends PostgreSQLTest {
  private static final Logger log = LoggerFactory.getLogger(PostgreSQLTableMetadataProviderTest.class);
  PostgreSQLSourceConnectorConfig config;
  PostgreSQLTableMetadataProvider tableMetadataProvider;

  @BeforeEach
  public void settings(@PostgreSQLSettings Map<String, String> settings) {
    this.config = new PostgreSQLSourceConnectorConfig(settings);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new PostgreSQLTableMetadataProvider(this.config, offsetStorageReader);
  }

  @TestFactory
  public Stream<DynamicTest> fetchTableMetadata(
      @DockerFormatString(container = PostgreSQLTestConstants.CONTAINER_NAME, port = PostgreSQLTestConstants.PORT, format = PostgreSQLTestConstants.JDBC_URL_FORMAT) String jdbcUrl
  ) throws SQLException {

    List<ChangeKey> tables = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSQLTestConstants.USERNAME, PostgreSQLTestConstants.PASSWORD)) {
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
    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSQLTestConstants.USERNAME, PostgreSQLTestConstants.PASSWORD)) {
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
