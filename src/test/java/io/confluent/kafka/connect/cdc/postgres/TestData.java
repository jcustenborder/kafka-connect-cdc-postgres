package io.confluent.kafka.connect.cdc.postgres;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.confluent.kafka.connect.cdc.JsonChange;
import io.confluent.kafka.connect.cdc.ObjectMapperFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class TestData {
  @JsonIgnore
  public String name;
  public String location;
  public Long xid;
  public long timestamp;
  public String data;
  public JsonTableMetadata tableMetadata;
  public JsonChange expected;

  public static void write(File file, TestData change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      write(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, TestData change) throws IOException {
    ObjectMapperFactory.instance.writeValue(outputStream, change);
  }

  public static TestData read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.instance.readValue(inputStream, TestData.class);
  }
}
