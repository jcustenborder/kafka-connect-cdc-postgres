package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import io.confluent.kafka.connect.utils.data.Parser;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

class ChangeParseTreeListener extends PgLogicalDecodingBaseListener {
  private static final Logger log = LoggerFactory.getLogger(ChangeParseTreeListener.class);
  final PostgreSQLSourceConnectorConfig config;
  final TableMetadataProvider tableMetadataProvider;
  final Parser parser;
  String tableName;
  String schemaName;
  PostgreSQLChange change;


  ChangeParseTreeListener(PostgreSQLSourceConnectorConfig config, TableMetadataProvider tableMetadataProvider) {
    this.config = config;
    this.tableMetadataProvider = tableMetadataProvider;
    this.parser = new Parser();
    this.parser.registerTypeParser(PostgreSQLConstants.pointSchema().build(), new Parsers.PointTypeParser());
    this.parser.registerTypeParser(PostgreSQLConstants.boxSchema().build(), new Parsers.BoxTypeParser());
    this.parser.registerTypeParser(PostgreSQLConstants.circleSchema().build(), new Parsers.CircleTypeParser());
    this.parser.registerTypeParser(PostgreSQLConstants.polygonSchema().build(), new Parsers.PolygonTypeParser());
    this.parser.registerTypeParser(PostgreSQLConstants.pathSchema().build(), new Parsers.PathTypeParser());
    this.parser.registerTypeParser(PostgreSQLConstants.lsegSchema().build(), new Parsers.LsegTypeParser());
  }

  @Override
  public void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx) {
    if (log.isTraceEnabled()) {
      log.trace("enterInsertOp - {}", ctx.toStringTree());
    }


    this.change = new PostgreSQLChange();
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

    if (log.isTraceEnabled()) {
      log.trace("{}: Processing columns.", changeKey);
    }
    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
  }

  private void parseColumns(List<PgLogicalDecodingParser.NewKeyValuePairContext> values, ChangeKey changeKey, TableMetadataProvider.TableMetadata tableMetadata) {
    for (PgLogicalDecodingParser.NewKeyValuePairContext kvp : values) {
      PostgreSQLChange.PostgreSQLColumnValue columnValue = new PostgreSQLChange.PostgreSQLColumnValue();
      this.change.valueColumns.add(columnValue);
      columnValue.columnName = kvp.columnname().getText();
      if (log.isTraceEnabled()) {
        log.trace("{}: Processing '{}'", changeKey, columnValue.columnName);
      }

      String typeDef = kvp.typedef().getText();

      if (log.isTraceEnabled()) {
        log.trace("{}: typedef='{}'", changeKey, typeDef);
      }

      String input=null;

      if (null != kvp.value()) {
        if (log.isTraceEnabled()) {
          log.trace("{}: value='{}'", changeKey, kvp.value().getText());
        }
        input = kvp.value().getText();
      } else if (null != kvp.quotedValue()) {
        if (log.isTraceEnabled()) {
          log.trace("{}: quotedValue='{}'", changeKey, kvp.quotedValue().getText());
        }
        input = kvp.quotedValue().getText();
        input = input.substring(1, input.length()-1);
        if(typeDef.equalsIgnoreCase("money") && input.startsWith("$")){
          input = input.substring(1);
        }
      }

      if(log.isTraceEnabled()) {
        log.trace("{}: input='{}'", changeKey, input);
      }
      Preconditions.checkNotNull(input, "input should never be null.");
      columnValue.schema = tableMetadata.columnSchemas().get(columnValue.columnName);

      if("null".equalsIgnoreCase(input)){
        columnValue.value = null;
        continue;
      }

      Set<String> skip = ImmutableSet.of("bytea", "money", "interval");
      if(skip.contains(typeDef)){
        continue;
      }

      columnValue.value = this.parser.parseString(columnValue.schema, input);

      if(tableMetadata.keyColumns().contains(columnValue.columnName)){
        if(log.isTraceEnabled()) {
          log.trace("{}: Found key column {}", changeKey, columnValue.columnName);
        }

        this.change.keyColumns.add(columnValue);
      }

      if (log.isTraceEnabled()) {
        log.trace("{}: Finished processing '{}'", changeKey, columnValue);
      }
    }


  }


  @Override
  public void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx) {
    if (log.isTraceEnabled()) {
      log.trace("enterUpdateOp - {}", ctx.toStringTree());
    }
    this.change = new PostgreSQLChange();
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

    if (log.isTraceEnabled()) {
      log.trace("{}: Processing columns.", changeKey);
    }

    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
  }

  @Override
  public void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx) {
    if (log.isTraceEnabled()) {
      log.trace("enterDeleteOp - {}", ctx.toStringTree());
    }
    this.change = new PostgreSQLChange();
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

    if (log.isTraceEnabled()) {
      log.trace("{}: Processing columns.", changeKey);
    }

    List<PgLogicalDecodingParser.NewKeyValuePairContext> values = ctx.newKeyValuePair();
    parseColumns(values, changeKey, tableMetadata);
    this.change.valueColumns.clear();
  }

  @Override
  public void enterSchemaname(PgLogicalDecodingParser.SchemanameContext ctx) {
    if (log.isTraceEnabled()) {
      log.trace("enterSchemaname - {}", ctx.toStringTree());
    }
    this.schemaName = ctx.Identifier().getText();
  }

  @Override
  public void enterTablename(PgLogicalDecodingParser.TablenameContext ctx) {
    if (log.isTraceEnabled()) {
      log.trace("enterTablename - {}", ctx.toStringTree());
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
