package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class PostgreSQLTest {

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = PostgreSQLTestConstants.CONTAINER_NAME, port = PostgreSQLTestConstants.PORT, format = PostgreSQLTestConstants.JDBC_URL_FORMAT) String jdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    createSlot(jdbcUrl);
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, PostgreSQLTestConstants.USERNAME, PostgreSQLTestConstants.PASSWORD);
    flyway.migrate();
  }

  static void createSlot(String jdbcUrl) throws SQLException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSQLTestConstants.USERNAME, PostgreSQLTestConstants.PASSWORD)) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT 'init' FROM pg_create_logical_replication_slot(?, ?)")) {
        statement.setString(1, PostgreSQLTestConstants.REPLICATION_SLOT_NAME);
        statement.setString(2, "test_decoding");
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {

          }
        }
      }
    }
  }
}
