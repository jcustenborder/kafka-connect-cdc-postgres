package io.confluent.kafka.connect.cdc.postgres.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.NamedTest;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PostgreSQLChangeTestCase implements NamedTest {
  @JsonIgnore
  private String name;

  public String location;
  public long xid;
  public String data;
  public Time time;

  public TableMetadataProvider.TableMetadata tableMetadata;
  public Change expected;

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }

  public static void write(File file, PostgreSQLChangeTestCase change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.instance.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, PostgreSQLChangeTestCase change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static PostgreSQLChangeTestCase read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, PostgreSQLChangeTestCase.class);
  }

  public static PostgreSQLChangeTestCase read(File inputFile) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(inputFile)) {
      return ObjectMapperFactory.instance.readValue(inputStream, PostgreSQLChangeTestCase.class);
    }
  }
}
