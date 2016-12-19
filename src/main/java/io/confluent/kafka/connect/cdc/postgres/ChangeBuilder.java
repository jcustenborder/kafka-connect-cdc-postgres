package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.cdc.Change;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

class ChangeBuilder {
  private static final Logger log = LoggerFactory.getLogger(ChangeBuilder.class);
  KeyMetadataProvider keyMetadataProvider;
  final String slotName;
  final Time time;


  ChangeBuilder(KeyMetadataProvider keyMetadataProvider, Time time, String slotName) {
    Preconditions.checkNotNull(keyMetadataProvider, "keyMetadataProvider cannot be null.");
    Preconditions.checkNotNull(time, "time cannot be null.");
    this.keyMetadataProvider = keyMetadataProvider;
    this.time = time;
    this.slotName = slotName;
  }

  PostgreSQLChange build(ResultSet results) throws SQLException {
    String location = results.getString(1);
    Long xid = results.getLong(2);
    String data = results.getString(3);

    if (log.isDebugEnabled()) {
      log.debug("location='{}' xid='{}' data='{}'", location, xid, data);
    }

    ChangeParseTreeListener listener = new ChangeParseTreeListener();
    CharStream inputStream = new ANTLRInputStream(data);
    PgLogicalDecodingLexer lexer = new PgLogicalDecodingLexer(inputStream);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    PgLogicalDecodingParser parser = new PgLogicalDecodingParser(tokens);
    PgLogicalDecodingParser.LoglineContext parseTree = parser.logline();
    ParseTreeWalker.DEFAULT.walk(listener, parseTree);
    PostgreSQLChange change = listener.change();
    change.sourceOffset(this.slotName, location);
    long timestamp = this.time.milliseconds();
    change.timestamp = timestamp;

    Set<String> keyColumns = this.keyMetadataProvider.keys(change.schemaName, change.tableName);

    for (Change.ColumnValue columnValue : change.valueColumns) {
      if (keyColumns.contains(columnValue.columnName())) {
        change.keyColumns.add(columnValue);
      }
    }

    return change;
  }
}
