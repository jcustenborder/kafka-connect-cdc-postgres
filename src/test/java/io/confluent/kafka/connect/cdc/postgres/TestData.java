package io.confluent.kafka.connect.cdc.postgres;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.connect.cdc.JsonChange;

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
  public JsonChange expected;

  final static ObjectMapper objectMapper;

  static {
    objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, true);
    objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    objectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  public static void write(File file, TestData change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      objectMapper.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, TestData change) throws IOException {
    objectMapper.writeValue(outputStream, change);
  }

  public static TestData read(InputStream inputStream) throws IOException {
    return objectMapper.readValue(inputStream, TestData.class);
  }
}
