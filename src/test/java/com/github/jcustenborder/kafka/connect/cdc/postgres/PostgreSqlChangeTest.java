/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.TestDataUtils;
import com.github.jcustenborder.kafka.connect.cdc.postgres.model.PostgreSQLChangeTestCase;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static com.github.jcustenborder.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.*;


public class PostgreSqlChangeTest {
  private static final Logger log = LoggerFactory.getLogger(PostgreSqlChangeTest.class);


  void build(PostgreSQLChangeTestCase testCase) throws SQLException {
    assertNotNull(testCase, "testCase should not be null.");
    ResultSet results = mock(ResultSet.class);
    when(results.getString(1)).thenReturn(testCase.location);
    when(results.getLong(2)).thenReturn(testCase.xid);
    when(results.getString(3)).thenReturn(testCase.data);
    TableMetadataProvider tableMetadataProvider = mock(TableMetadataProvider.class);
    when(tableMetadataProvider.tableMetadata(any(ChangeKey.class))).thenReturn(testCase.tableMetadata);
    PostgreSqlSourceConnectorConfig config = new PostgreSqlSourceConnectorConfig(PostgreSqlTestConstants.settings("dummy", 54321));
    PostgreSqlChange.Builder builder = new PostgreSqlChange.Builder(config, testCase.time, tableMetadataProvider);

    PostgreSqlChange actual = builder.build(results);
    verify(testCase.time, atLeastOnce()).milliseconds();

    if (log.isInfoEnabled()) {
      log.trace("expected= {}", testCase.expected);
      log.trace("testcase.expected.valuecolumns={}", testCase.expected.valueColumns());
      for (Change.ColumnValue cv : testCase.expected.valueColumns()) {
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
