package io.confluent.kafka.connect.cdc.postgres;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.kafka.connect.data.Schema;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JsonTableMetadata implements TableMetadataProvider.TableMetadata {
  String schemaName;
  String tableName;
  Set<String> keyColumns = new LinkedHashSet<>();
  Map<String, Schema> columnSchemas = new LinkedHashMap<>();

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  @Override
  public Set<String> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public Map<String, Schema> columnSchemas() {
    return this.columnSchemas;
  }
}
