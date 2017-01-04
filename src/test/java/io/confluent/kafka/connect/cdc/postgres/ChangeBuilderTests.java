package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeBuilderTests {
  static final String SLOT_NAME = "testing";
  ChangeBuilder changeBuilder;
  TableMetadataProvider tableMetadataProvider;
  Time time;

  @BeforeEach
  public void before() {
    this.tableMetadataProvider = mock(TableMetadataProvider.class);
    this.time = mock(Time.class);
    this.changeBuilder = new ChangeBuilder(this.tableMetadataProvider, time, SLOT_NAME);
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
    assertNotNull(testData, "testData cannot be null");
    assertNotNull(testData.tableMetadata, "testData.tableMetadata cannot be null");
    ResultSet resultSet = mockResultSet(testData);
    when(this.time.milliseconds()).thenReturn(testData.timestamp);
    when(this.tableMetadataProvider.tableMetadata(new ChangeKey(testData.tableMetadata.databaseName(), testData.tableMetadata.schemaName(), testData.tableMetadata.tableName()))).thenReturn(testData.tableMetadata);
    PostgreSQLChange actual = this.changeBuilder.build(resultSet);
    assertChange(testData.expected, actual);
  }


  @Disabled
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
}
