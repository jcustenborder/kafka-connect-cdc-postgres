/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeKey;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

class ChangeParseTreeListener extends PgLogicalDecodingBaseListener {
  private static final Logger log = LoggerFactory.getLogger(ChangeParseTreeListener.class);
  final PostgreSqlSourceConnectorConfig config;
  final TableMetadataProvider tableMetadataProvider;
  final Parser parser;
  String tableName;
  String schemaName;
  PostgreSqlChange change;


  ChangeParseTreeListener(PostgreSqlSourceConnectorConfig config, TableMetadataProvider tableMetadataProvider) {
    this.config = config;
    this.tableMetadataProvider = tableMetadataProvider;
    this.parser = new Parser();
    this.parser.registerTypeParser(PostgreSqlConstants.pointSchema().build(), new Parsers.PointTypeParser());
    this.parser.registerTypeParser(PostgreSqlConstants.boxSchema().build(), new Parsers.BoxTypeParser());
    this.parser.registerTypeParser(PostgreSqlConstants.circleSchema().build(), new Parsers.CircleTypeParser());
    this.parser.registerTypeParser(PostgreSqlConstants.polygonSchema().build(), new Parsers.PolygonTypeParser());
    this.parser.registerTypeParser(PostgreSqlConstants.pathSchema().build(), new Parsers.PathTypeParser());
    this.parser.registerTypeParser(PostgreSqlConstants.lsegSchema().build(), new Parsers.LsegTypeParser());
  }

  @Override
  public void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx) {
    log.trace("enterInsertOp - {}", ctx.toStringTree());


    this.change = new PostgreSqlChange();
    this.change.changeType(Change.ChangeType.INSERT);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);

    ChangeKey changeKey = new ChangeKey(this.config.initialDatabase, this.schemaName, this.tableName);
    TableMetadataProvider.TableMetadata tableMetadata;
    try {
      tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
    } catch (SQLException e) {
      throw new DataException("Exception thrown while querying " + changeKey, e);
    }

    log.trace("{}: Processing columns.", changeKey);
    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
  }

  private void parseColumns(List<PgLogicalDecodingParser.NewKeyValuePairContext> values, ChangeKey changeKey, TableMetadataProvider.TableMetadata tableMetadata) {
    for (PgLogicalDecodingParser.NewKeyValuePairContext kvp : values) {
      PostgreSqlChange.PostgreSQLColumnValue columnValue = new PostgreSqlChange.PostgreSQLColumnValue();
      this.change.valueColumns.add(columnValue);
      columnValue.columnName = kvp.columnname().getText();
      log.trace("{}: Processing '{}'", changeKey, columnValue.columnName);

      String typeDef = kvp.typedef().getText();

      log.trace("{}: typedef='{}'", changeKey, typeDef);

      String input = null;

      if (null != kvp.value()) {
        log.trace("{}: value='{}'", changeKey, kvp.value().getText());
        input = kvp.value().getText();
      } else if (null != kvp.quotedValue()) {
        log.trace("{}: quotedValue='{}'", changeKey, kvp.quotedValue().getText());
        input = kvp.quotedValue().getText();
        input = input.substring(1, input.length() - 1);
        if (typeDef.equalsIgnoreCase("money") && input.startsWith("$")) {
          input = input.substring(1);
        }
      }

      log.trace("{}: input='{}'", changeKey, input);
      Preconditions.checkNotNull(input, "input should never be null.");
      columnValue.schema = tableMetadata.columnSchemas().get(columnValue.columnName);

      if ("null".equalsIgnoreCase(input)) {
        columnValue.value = null;
        continue;
      }

      Set<String> skip = ImmutableSet.of("bytea", "money", "interval");
      if (skip.contains(typeDef)) {
        continue;
      }

      columnValue.value = this.parser.parseString(columnValue.schema, input);

      if (tableMetadata.keyColumns().contains(columnValue.columnName)) {
        log.trace("{}: Found key column {}", changeKey, columnValue.columnName);

        this.change.keyColumns.add(columnValue);
      }

      log.trace("{}: Finished processing '{}'", changeKey, columnValue);
    }


  }


  @Override
  public void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx) {
    log.trace("enterUpdateOp - {}", ctx.toStringTree());
    this.change = new PostgreSqlChange();
    this.change.changeType(Change.ChangeType.UPDATE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);

    ChangeKey changeKey = new ChangeKey(this.config.initialDatabase, this.schemaName, this.tableName);
    TableMetadataProvider.TableMetadata tableMetadata;
    try {
      tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
    } catch (SQLException e) {
      throw new DataException("Exception thrown while querying " + changeKey, e);
    }

    log.trace("{}: Processing columns.", changeKey);

    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
  }

  @Override
  public void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx) {
    log.trace("enterDeleteOp - {}", ctx.toStringTree());
    this.change = new PostgreSqlChange();
    this.change.changeType(Change.ChangeType.DELETE);
    this.change.schemaName(schemaName);
    this.change.tableName(tableName);

    ChangeKey changeKey = new ChangeKey(this.config.initialDatabase, this.schemaName, this.tableName);
    TableMetadataProvider.TableMetadata tableMetadata;
    try {
      tableMetadata = this.tableMetadataProvider.tableMetadata(changeKey);
    } catch (SQLException e) {
      throw new DataException("Exception thrown while querying " + changeKey, e);
    }

    log.trace("{}: Processing columns.", changeKey);

    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
    this.change.valueColumns.clear();
  }

  @Override
  public void enterSchemaname(PgLogicalDecodingParser.SchemanameContext ctx) {
    log.trace("enterSchemaname - {}", ctx.toStringTree());
    this.schemaName = ctx.Identifier().getText();
  }

  @Override
  public void enterTablename(PgLogicalDecodingParser.TablenameContext ctx) {
    log.trace("enterTablename - {}", ctx.toStringTree());
    this.tableName = ctx.Identifier().getText();
  }

  @Override
  public void visitErrorNode(ErrorNode node) {


    super.visitErrorNode(node);
  }

  public PostgreSqlChange change() {
    return this.change;
  }
}
