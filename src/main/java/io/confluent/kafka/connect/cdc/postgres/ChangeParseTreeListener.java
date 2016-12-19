package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Change;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class ChangeParseTreeListener extends PgLogicalDecodingBaseListener {
  private static final Logger log = LoggerFactory.getLogger(ChangeParseTreeListener.class);

  String tableName;
  String schemaName;
  PostgreSQLChange change;

  @Override
  public void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx) {
    if(log.isDebugEnabled()) {
      log.debug("enterInsertOp - {}", ctx.toStringTree());
    }
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.INSERT);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);

    List<PgLogicalDecodingParser.NewKeyValuePairContext> a = ctx.newKeyValuePair();
    for(PgLogicalDecodingParser.NewKeyValuePairContext kvp:a) {
      PostgreSQLColumnValue columnValue = new PostgreSQLColumnValue();
      columnValue.columnName = kvp.columnname().Identifier().getText();

      this.change.valueColumns.add(columnValue);
    }
  }

  @Override
  public void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx) {
    if(log.isDebugEnabled()) {
      log.debug("enterUpdateOp - {}", ctx.toStringTree());
    }
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.UPDATE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);

    List<PgLogicalDecodingParser.NewKeyValuePairContext> a = ctx.newKeyValuePair();
    for(PgLogicalDecodingParser.NewKeyValuePairContext kvp:a) {
      PostgreSQLColumnValue columnValue = new PostgreSQLColumnValue();
      columnValue.columnName = kvp.columnname().Identifier().getText();
      this.change.valueColumns.add(columnValue);
    }
  }

  @Override
  public void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx) {
    if(log.isDebugEnabled()) {
      log.debug("enterDeleteOp - {}", ctx.toStringTree());
    }
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.DELETE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);
  }

  @Override
  public void enterSchemaname(PgLogicalDecodingParser.SchemanameContext ctx) {
    if(log.isDebugEnabled()) {
      log.debug("enterSchemaname - {}", ctx.toStringTree());
    }
    this.schemaName = ctx.Identifier().getText();
  }

  @Override
  public void enterTablename(PgLogicalDecodingParser.TablenameContext ctx) {
    if(log.isDebugEnabled()) {
      log.debug("enterTablename - {}", ctx.toStringTree());
    }
    this.tableName = ctx.Identifier().getText();
  }

  @Override
  public void visitErrorNode(ErrorNode node) {


    super.visitErrorNode(node);
  }

  public PostgreSQLChange change() {
    return this.change;
  }
}
