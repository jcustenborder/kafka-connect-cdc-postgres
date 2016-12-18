package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Change;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.sql.ResultSet;
import java.sql.SQLException;

class ChangeBuilder {
  final String slotName;
  ChangeParseTreeListener listener = new ChangeParseTreeListener();

  ChangeBuilder(String slotName) {
    this.slotName = slotName;
  }

  Change build(ResultSet results) throws SQLException {
    String location = results.getString(1);
    Long xid = results.getLong(2);
    String data = results.getString(3);

    CharStream inputStream = new ANTLRInputStream(data);
    PgLogicalDecodingLexer lexer = new PgLogicalDecodingLexer(inputStream);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    PgLogicalDecodingParser parser = new PgLogicalDecodingParser(tokens);
    PgLogicalDecodingParser.LoglineContext parseTree = parser.logline();
    ParseTreeWalker.DEFAULT.walk(listener, parseTree);
    PostgreSQLChange change = listener.change();
    change.sourceOffset(this.slotName, location);


    return change;
  }
}
