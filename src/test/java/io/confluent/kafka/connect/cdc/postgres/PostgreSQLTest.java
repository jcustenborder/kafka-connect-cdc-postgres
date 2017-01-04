package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.docker.DockerFormatString;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.sql.SQLException;

public abstract class PostgreSQLTest {

  @BeforeAll
  public static void beforeClass(
      @DockerFormatString(container = PostgreSQLConstants.CONTAINER_NAME, port = PostgreSQLConstants.PORT, format = PostgreSQLConstants.JDBC_URL_FORMAT) String jdbcUrl
  ) throws SQLException, InterruptedException, IOException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, PostgreSQLConstants.USERNAME, PostgreSQLConstants.PASSWORD);
    flyway.migrate();
  }


}
