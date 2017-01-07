package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.cdc.TestDataUtils;
import io.confluent.kafka.connect.cdc.postgres.model.PostgreSQLChangeTestCase;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.*;


public class PostgreSQLChangeTest {
  private static final Logger log = LoggerFactory.getLogger(PostgreSQLChangeTest.class);


  void build(PostgreSQLChangeTestCase testCase) throws SQLException {
    assertNotNull(testCase, "testCase should not be null.");
    ResultSet results = mock(ResultSet.class);
    when(results.getString(1)).thenReturn(testCase.location);
    when(results.getLong(2)).thenReturn(testCase.xid);
    when(results.getString(3)).thenReturn(testCase.data);
    TableMetadataProvider tableMetadataProvider = mock(TableMetadataProvider.class);
    when(tableMetadataProvider.tableMetadata(any(ChangeKey.class))).thenReturn(testCase.tableMetadata);
    PostgreSQLSourceConnectorConfig config = new PostgreSQLSourceConnectorConfig(PostgreSQLTestConstants.settings("dummy", 54321));
    PostgreSQLChange.Builder builder = new PostgreSQLChange.Builder(config, testCase.time, tableMetadataProvider);

    PostgreSQLChange actual = builder.build(results);
    verify(testCase.time, atLeastOnce()).milliseconds();

    if(log.isInfoEnabled()) {
      log.trace("expected= {}", testCase.expected);
      log.trace("testcase.expected.valuecolumns={}", testCase.expected.valueColumns());
      for(Change.ColumnValue cv:testCase.expected.valueColumns()){
        log.trace("columnvalue[{}]={}", cv.columnName(), cv.value());
      }
    }

    assertChange(testCase.expected, actual);
  }

  @TestFactory
  public Stream<DynamicTest> build() throws IOException {
    String packageName = this.getClass().getPackage().getName() + ".change";
    List<PostgreSQLChangeTestCase> testCases = TestDataUtils.loadJsonResourceFiles(packageName, PostgreSQLChangeTestCase.class);
    return testCases.stream().map(data -> dynamicTest(data.name(), () -> build(data)));
  }
}
