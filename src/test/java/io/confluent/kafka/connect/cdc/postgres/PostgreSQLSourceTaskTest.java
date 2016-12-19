package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.JsonColumnValue;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.postgres.docker.DockerUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.flywaydb.core.Flyway;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgreSQLSourceTaskTest {
  @ClassRule
  public final static DockerComposeRule docker = DockerUtils.postgresql();
  public static Container postgreSQLContainer;
  public static String jdbcUrl;

  @BeforeAll
  public static void beforeClass() throws SQLException, InterruptedException, IOException {
    docker.before();
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

  @BeforeEach
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


  @Disabled
  @Test
  public void decimalTestCase() throws IOException, SQLException {
    final int precision = 50;
    try (Connection connection = DriverManager.getConnection(jdbcUrl, DockerUtils.USERNAME, DockerUtils.PASSWORD)) {

      Multimap<String, BigDecimal> keyToValues = ArrayListMultimap.create();
      for (int scale = 0; scale < 51; scale++) {
        String tableName = String.format("DECIMAL_%d_%d", precision, scale);
        String insertSQL = String.format("INSERT INTO %s(value) values (?)", tableName);

        Long id = null;
        try (PreparedStatement insertStatement = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {
          BigDecimal decimal = TestDataUtils.randomBigDecimal(scale);
          insertStatement.setBigDecimal(1, decimal);
          insertStatement.execute();
          try (ResultSet keyResults = insertStatement.getGeneratedKeys()) {
            while (keyResults.next()) {
              id = keyResults.getLong(1);
            }
          }

          keyToValues.put(tableName, decimal);
        }

        tableName = String.format("DECIMAL_%d_%d", precision, scale);
        String updateSQL = String.format("UPDATE %s SET value = ? where ID = ?", tableName);

        try (PreparedStatement insertStatement = connection.prepareStatement(updateSQL)) {
          BigDecimal decimal = TestDataUtils.randomBigDecimal(scale);
          insertStatement.setBigDecimal(1, decimal);
          insertStatement.setLong(2, id);
          insertStatement.executeUpdate();
          keyToValues.put(tableName, decimal);
        }
      }


      File parentFile = new File("/Users/jeremy/source/confluent/kafka-connect/public/kafka-connect-cdc/kafka-connect-cdc-postgres/src/test/resources/io/confluent/kafka/connect/cdc/postgres/decimal");

      try (ResultSet results = this.task.queryChanges()) {
        int insert = 0, update = 0;

        Pattern tablePattern = Pattern.compile("^table public.(decimal_(\\d+)_(\\d+)): (INSERT|UPDATE)");

        while (results.next()) {
          String location = results.getString(1);
          Long xid = results.getLong(2);
          String data = results.getString(3);

          TestData testData = new TestData();
          testData.location = location;
          testData.data = data;
          testData.xid = xid;
          testData.timestamp = System.currentTimeMillis();
          File file;

          Matcher matcher = tablePattern.matcher(data);

          if (!matcher.find()) {
            continue;
          }

          String tableName = matcher.group(1);
          String changeType = matcher.group(4);
          int scale = Integer.parseInt(matcher.group(3));
          Schema valueSchema = Decimal.builder(scale).optional().build();

          testData.expected = new JsonChange();
          testData.tableMetadata = new JsonTableMetadata();
          testData.tableMetadata.keyColumns().add("id");
          testData.tableMetadata.schemaName = "public";
          testData.tableMetadata.tableName = tableName;
          testData.tableMetadata.columnSchemas.put("id", Schema.OPTIONAL_INT64_SCHEMA);
          testData.tableMetadata.columnSchemas.put("value", valueSchema);

          Collection<BigDecimal> values = keyToValues.get(tableName.toUpperCase());
          BigDecimal expectedValue = null;
          for (BigDecimal v : values) {
            expectedValue = v;
          }
          values.remove(expectedValue);

          testData.expected.timestamp(testData.timestamp);
          testData.expected.schemaName("public");
          testData.expected.tableName(tableName);
          testData.expected.sourceOffset().put("testing", location);
          testData.expected.keyColumns().add(new JsonColumnValue("id", Schema.OPTIONAL_INT64_SCHEMA, 1));
          testData.expected.valueColumns().add(new JsonColumnValue("id", Schema.OPTIONAL_INT64_SCHEMA, 1));
          testData.expected.valueColumns().add(new JsonColumnValue("value", valueSchema, expectedValue));

          if (changeType.equals("UPDATE")) {
            update++;
            testData.expected.changeType(Change.ChangeType.UPDATE);
            String filename = String.format("update_%03d.json", update);
            file = new File(parentFile, filename);

          } else if (changeType.equals("INSERT")) {
            insert++;
            testData.expected.changeType(Change.ChangeType.INSERT);
            String filename = String.format("insert_%03d.json", insert);
            file = new File(parentFile, filename);
          } else {
            continue;
          }
          TestData.write(file, testData);
        }
      }

    }
  }

  //  @Test
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

    this.task.processChanges();


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
