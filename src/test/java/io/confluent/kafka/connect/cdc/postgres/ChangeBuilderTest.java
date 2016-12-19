package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.FilterBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.confluent.kafka.connect.cdc.ChangeAssertions.assertChange;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeBuilderTest {
  static final String SLOT_NAME = "testing";
  ChangeBuilder changeBuilder;

  @BeforeEach
  public void before() {
    this.changeBuilder = new ChangeBuilder(SLOT_NAME);
  }

  ResultSet mockResultSet(String location, Long xid, String data) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn(location);
    when(resultSet.getLong(2)).thenReturn(xid);
    when(resultSet.getString(3)).thenReturn(data);
    return resultSet;
  }

  ResultSet mockResultSet(TestData testData) throws SQLException {
    return mockResultSet(testData.location, testData.xid, testData.data);
  }

  void test(TestData testData) throws SQLException {
    ResultSet resultSet = mockResultSet(testData);
    PostgreSQLChange actual = this.changeBuilder.build(resultSet);
    actual.timestamp = testData.timestamp;

    assertChange(testData.expected, actual);
  }


  @TestFactory
  public Stream<DynamicTest> build() throws IOException {
    String packageName = this.getClass().getPackage().getName();
    Reflections reflections = new Reflections(packageName, new ResourcesScanner());
    Set<String> resources = reflections.getResources(new FilterBuilder.Include(".*"));
    List<TestData> testDatas = new ArrayList<>(resources.size());

    Path packagePath = Paths.get("/" + packageName.replace(".", "/"));

    for (String resource : resources) {
      Path resourcePath = Paths.get("/" + resource);
      Path relativePath = packagePath.relativize(resourcePath);
      File resourceFile = new File("/" + resource);
      InputStream inputStream = this.getClass().getResourceAsStream(resourceFile.getAbsolutePath());
      TestData testData = TestData.read(inputStream);

      String nameWithoutExtension = Files.getNameWithoutExtension(resource);
      if (null != relativePath.getParent()) {
        String parentName = relativePath.getParent().getFileName().toString();
        testData.name = parentName + "/" + nameWithoutExtension;
      } else {
        testData.name = nameWithoutExtension;
      }

      testDatas.add(testData);
    }

    return testDatas.stream().map(data -> dynamicTest(data.name, () -> test(data)));
  }

  @Disabled
  @Test
  public void decimalTestCase() throws IOException {
    final int precision = 50;
    try (BufferedWriter writer = new BufferedWriter(Files.newWriter(new File("/Users/jeremy/source/confluent/kafka-connect/public/kafka-connect-cdc/kafka-connect-cdc-postgres/src/test/resources/db/migration/V1_2__decimal.sql"), Charsets.UTF_8))) {
      for (int scale = 0; scale < 51; scale++) {
        String createStatement = String.format("CREATE TABLE DECIMAL_%d_%d(ID BIGSERIAL PRIMARY KEY, VALUE DECIMAL(%d, %d));", precision, scale, precision, scale);
        writer.write(createStatement);
        writer.newLine();
      }

      writer.newLine();

      writer.write("CREATE TABLE ALL_DECIMALS(");
      writer.newLine();
      writer.write("  ID BIGSERIAL PRIMARY KEY");

      for (int scale = 0; scale < 51; scale++) {
        writer.write(",");
        writer.newLine();

        String column = String.format("  DECIMAL_%d_%d DECIMAL(%d, %d)", precision, scale, precision, scale);
        writer.write(column);
      }

      writer.newLine();
      writer.write(")");
      writer.newLine();
    }
  }

//  @Test
//  public void insert_00() throws SQLException, IOException {
//    TestData testData = new TestData();
//    testData.location = "0/151CDF0";
//    testData.xid = 555L;
//    testData.data = "table public.simple_table: INSERT: user_id[bigint]:1 email_address[character varying]:'hinton@yahoo.com' first_name[character varying]:'Anthony' last_name[character varying]:'Hinton' description[text]:'Pain but example, pain, to. No there great. But physical pleasure?. Expound No which pain of fault. Not has produces take all pleasure.''Has it ever. Man who occur. Who But teachings pain rejects, expound pleasure, occasionally happiness. Of pleasure. Pursues because because.'";
//
//
//    JsonChange jsonChange = new JsonChange();
//
//
//    final String OFFSET = "0/151CDF0";
//    ResultSet input = mockResultSet(OFFSET, 555L, "table public.simple_table: INSERT: user_id[bigint]:1 email_address[character varying]:'hinton@yahoo.com' first_name[character varying]:'Anthony' last_name[character varying]:'Hinton' description[text]:'Pain but example, pain, to. No there great. But physical pleasure?. Expound No which pain of fault. Not has produces take all pleasure.''Has it ever. Man who occur. Who But teachings pain rejects, expound pleasure, occasionally happiness. Of pleasure. Pursues because because.'");
//    Change change = this.changeBuilder.build(input);
//    assertNotNull("Change should not be null.", change);
//    assertEquals("changeType does not match.", Change.ChangeType.INSERT, change.changeType());
//    assertEquals("schemaName does not match", "public", change.schemaName());
//    assertEquals("table does not match", "simple_table", change.tableName());
//    assertNotNull("keyColumns should not be null.", change.keyColumns());
//    assertNotNull("valueColumns should not be null.", change.valueColumns());
//
////    final Map<String, Object> expectedSourceOffset = ImmutableMap.of(SLOT_NAME, (Object) OFFSET);
////    assertThat("sourceOffset does not match.", change.sourceOffset(), IsEqual.equalTo(expectedSourceOffset));
////
////    final Map<String, Object> expectedSourcePartition = ImmutableMap.of();
////    assertThat("sourcePartition does not match.", change.sourcePartition(), IsEqual.equalTo(expectedSourcePartition));
//
//    Map<String, PostgreSQLColumnValue> keyColumns;
//  }

}
