package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.Change;

class ChangeParseTreeListener extends PgLogicalDecodingBaseListener {
  String tableName;
  String schemaName;

  PostgreSQLChange change;

  @Override
  public void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx) {
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.INSERT);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);
  }

  @Override
  public void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx) {
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.UPDATE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);
  }

  @Override
  public void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx) {
    this.change = new PostgreSQLChange();
    this.change.changeType(Change.ChangeType.DELETE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);
  }

  @Override
  public void enterSchemaname(PgLogicalDecodingParser.SchemanameContext ctx) {
    this.schemaName = ctx.Identifier().getText();
  }

  @Override
  public void enterTablename(PgLogicalDecodingParser.TablenameContext ctx) {
    this.tableName = ctx.Identifier().getText();
  }

  public PostgreSQLChange change() {
    return this.change;
  }
}
