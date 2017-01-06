package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.utils.data.Parser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PostgreSQLChange implements Change {
  String databaseName;
  String tableName;
  String schemaName;
  ChangeType changeType;
  Map<String, Object> sourceOffset;
  Map<String, String> metadata = new LinkedHashMap<>();
  Map<String, Object> sourcePartition;

  long timestamp;


  List<ColumnValue> keyColumns = new ArrayList<>();
  List<ColumnValue> valueColumns = new ArrayList<>();

  @Override
  public Map<String, String> metadata() {
    return this.metadata;
  }

  @Override
  public Map<String, Object> sourcePartition() {
    return this.sourcePartition;
  }

  @Override
  public Map<String, Object> sourceOffset() {
    return this.sourceOffset;
  }

  @Override
  public String databaseName() {
    return this.databaseName;
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
    return this.timestamp;
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

  static class Builder {
    private static final Logger log = LoggerFactory.getLogger(Builder.class);
    final TableMetadataProvider tableMetadataProvider;
    final PostgreSQLSourceConnectorConfig config;
    final Time time;
    final Map<String, Object> sourcePartition;

    Builder(PostgreSQLSourceConnectorConfig config, Time time, TableMetadataProvider tableMetadataProvider) {
      this.config = config;
      Preconditions.checkNotNull(time, "time cannot be null.");
      Preconditions.checkNotNull(tableMetadataProvider, "tableMetadataProvider cannot be null.");
      this.tableMetadataProvider = tableMetadataProvider;
      this.time = time;
      this.sourcePartition = ImmutableMap.of("slot", this.config.replicationSlotName);
    }

    PostgreSQLChange build(ResultSet results) throws SQLException {
      String location = results.getString(1);
      Long xid = results.getLong(2);
      String data = results.getString(3);

      if (log.isTraceEnabled()) {
        log.trace("location='{}' xid='{}' data='{}'", location, xid, data);
      }

      if (data.startsWith("BEGIN")) {
        if (log.isTraceEnabled()) {
          log.trace("Skipping records because of begin commit.");
        }
        return null;
      }

      ChangeParseTreeListener listener = new ChangeParseTreeListener(config, this.tableMetadataProvider);
      CharStream inputStream = new ANTLRInputStream(data);

      PgLogicalDecodingLexer lexer = new PgLogicalDecodingLexer(inputStream);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      PgLogicalDecodingParser parser = new PgLogicalDecodingParser(tokens);
      parser.setErrorHandler(new BailErrorStrategy());
      PgLogicalDecodingParser.LoglineContext parseTree = parser.logline();
      ParseTreeWalker.DEFAULT.walk(listener, parseTree);
      PostgreSQLChange change = listener.change();

      if(null!=change) {
        change.timestamp = this.time.milliseconds();
        change.metadata = ImmutableMap.of(
            "location", location,
            "xid", xid.toString()
        );
        change.sourceOffset = ImmutableMap.of(
            "location", location
        );
        change.sourcePartition = this.sourcePartition;
        change.databaseName = this.config.initialDatabase;
      }


      return change;
    }
  }

  static class PostgreSQLColumnValue implements ColumnValue {
    String columnName;
    Schema schema;
    Object value;

    @Override
    public String columnName() {
      return this.columnName;
    }

    @Override
    public Schema schema() {
      return this.schema;
    }

    @Override
    public Object value() {
      return this.value;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ColumnValue)) {
        return false;
      }

      ColumnValue that = (ColumnValue) obj;
      return
          this.columnName().equals(that.columnName()) &&
              this.schema.type().equals(that.schema().type()) &&
              this.schema.isOptional() == that.schema().isOptional() &&
              this.value() == that.value()
          ;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(PostgreSQLColumnValue.class)
          .omitNullValues()
          .add("columnName", this.columnName)
          .add("schema", this.schema)
          .add("value", this.value)
          .toString();
    }
  }
}
