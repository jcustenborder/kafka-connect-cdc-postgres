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

public abstract class PostgreSqlTest {

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = PostgreSqlTestConstants.CONTAINER_NAME, port = PostgreSqlTestConstants.PORT, format = PostgreSqlTestConstants.JDBC_URL_FORMAT) String jdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    createSlot(jdbcUrl);
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, PostgreSqlTestConstants.USERNAME, PostgreSqlTestConstants.PASSWORD);
    flyway.migrate();
  }

  static void createSlot(String jdbcUrl) throws SQLException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, PostgreSqlTestConstants.USERNAME, PostgreSqlTestConstants.PASSWORD)) {
      try (PreparedStatement statement = connection.prepareStatement("SELECT 'init' FROM pg_create_logical_replication_slot(?, ?)")) {
        statement.setString(1, PostgreSqlTestConstants.REPLICATION_SLOT_NAME);
        statement.setString(2, "test_decoding");
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {

          }
        }
      }
    }
  }
}
