package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Change;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class PostgreSQLChange implements Change {
  String tableName;
  String schemaName;
  ChangeType changeType;
  Map<String, Object> sourceOffset;
  static final Map<String, Object> SOURCE_PARTITION = ImmutableMap.of();

  List<ColumnValue> keyColumns = new ArrayList<>();
  List<ColumnValue> valueColumns = new ArrayList<>();


  @Override
  public Map<String, String> metadata() {
    return null;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return SOURCE_PARTITION;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
  }

  @Override
  public String schemaName() {
    return this.schemaName;
  }

  @Override
  public String tableName() {
    return this.tableName;
  }

  @Override
  public List<ColumnValue> keyColumns() {
    return this.keyColumns;
  }

  @Override
  public List<ColumnValue> valueColumns() {
    return this.valueColumns;
  }

  @Override
  public ChangeType changeType() {
    return this.changeType;
  }

  @Override
  public long timestamp() {
    return 0;
  }

  void tableName(String value) {
    this.tableName = value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("schemaName", this.schemaName)
        .add("tableName", this.tableName)
        .add("changeType", this.changeType)

        .toString();
  }

  void schemaName(String value) {
    this.schemaName = value;
  }

  void changeType(ChangeType insert) {
    this.changeType = insert;
  }

  void sourceOffset(String slotName, String location) {
    this.sourceOffset = ImmutableMap.of(slotName, (Object) location);
  }
}
