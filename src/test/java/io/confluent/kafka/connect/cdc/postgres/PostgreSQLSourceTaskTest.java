package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import io.confluent.kafka.connect.cdc.postgres.docker.DockerUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.ClassRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Disabled
public class PostgreSQLSourceTaskTest {
  @ClassRule
  public final static DockerComposeRule docker = DockerUtils.postgresql();
  public static Container postgreSQLContainer;
  public static String jdbcUrl;

  @BeforeAll
  public static void beforeClass() throws SQLException, InterruptedException, IOException {
    postgreSQLContainer = DockerUtils.postgreSQL(docker);
    jdbcUrl = DockerUtils.jdbcUrl(docker);
    flywayMigrate();
  }

  static void flywayMigrate() throws SQLException {
    Flyway flyway = new Flyway();
    flyway.setDataSource(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD);
//    flyway.setSchemas("CDC_TESTING");
    flyway.migrate();
  }

  PostgreSQLSourceTask task;

  @Before
  public void start() {
    Map<String, String> settings = ImmutableMap.of(
        PostgreSQLSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
        PostgreSQLSourceConnectorConfig.JDBC_PASSWORD_CONF, DockerUtils.PASSWORD,
        PostgreSQLSourceConnectorConfig.JDBC_USERNAME_CONF, DockerUtils.USERNAME,
        PostgreSQLSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAMES_CONF, "testing"
    );

    this.task = new PostgreSQLSourceTask();
    this.task.start(settings);
  }

  @Test
  public void queryChanges() throws SQLException, IOException {
    Multimap<Long, String> idToEmailAddress = LinkedListMultimap.create();

    Fairy fairy = Fairy.create();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD)) {
      String SQL = "INSERT INTO SIMPLE_TABLE(email_address, first_name, last_name, description) values (?, ?, ?, ?)";
      try (PreparedStatement insertStatement = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
        for (int i = 0; i < 10; i++) {
          Person person = fairy.person();
          insertStatement.setString(1, person.getEmail());
          insertStatement.setString(2, person.getFirstName());
          insertStatement.setString(3, person.getLastName());
          insertStatement.setString(4, fairy.textProducer().paragraph() + "'" + fairy.textProducer().paragraph());
          insertStatement.execute();
          Long id = null;
          try (ResultSet keyResults = insertStatement.getGeneratedKeys()) {
            while (keyResults.next()) {
              id = keyResults.getLong(1);
            }
          }
          idToEmailAddress.put(id, person.getEmail());
        }
      }
      SQL = "UPDATE SIMPLE_TABLE SET email_address = ? WHERE user_id = ?";
      try (PreparedStatement updateStatement = connection.prepareStatement(SQL)) {
        for (Long id : idToEmailAddress.keySet()) {
          Person person = fairy.person();
          String emailAddress = person.getEmail();
          updateStatement.setString(1, emailAddress);
          updateStatement.setLong(2, id);
          updateStatement.execute();
          idToEmailAddress.put(id, emailAddress);
        }
      }
    }

    this.task.queryChanges();


  }

  @AfterEach
  public void stop() {
    this.task.stop();
  }

  @AfterAll
  public static void dockerCleanup() {
    docker.after();
  }

}
