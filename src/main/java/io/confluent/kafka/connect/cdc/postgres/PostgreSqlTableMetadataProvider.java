package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.CachingTableMetadataProvider;
import io.confluent.kafka.connect.cdc.Change;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.JdbcUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

class PostgreSqlTableMetadataProvider extends CachingTableMetadataProvider<PostgreSqlSourceConnectorConfig> {
  final static String COLUMN_SQL = "SELECT " +
      "  column_name, " +
      "  is_nullable, " +
      "  data_type, " +
      "  numeric_scale " +
      "FROM " +
      "  information_schema.columns " +
      "WHERE " +
      "  lower(table_catalog) = lower(?) AND " +
      "  lower(table_schema) = lower(?) AND " +
      "  lower(columns.table_name) = lower(?) " +
      "ORDER BY " +
      "  ordinal_position";
  static final Map<String, Schema.Type> TYPE_LOOKUP;
  static final String PRIMARY_KEY_SQL = "SELECT\n" +
      "  pg_attribute.attname as column_name\n" +
      "FROM\n" +
      "  pg_index,\n" +
      "  pg_class,\n" +
      "  pg_attribute,\n" +
      "  pg_namespace\n" +
      "WHERE\n" +
      "  nspname = ? AND\n" +
      "  pg_class.oid = ?::REGCLASS AND\n" +
      "  indrelid = pg_class.oid AND\n" +
      "  pg_class.relnamespace = pg_namespace.oid AND\n" +
      "  pg_attribute.attrelid = pg_class.oid AND\n" +
      "  pg_attribute.attnum = ANY (pg_index.indkey)\n" +
      "  AND indisprimary";
  private static final Logger log = LoggerFactory.getLogger(PostgreSqlTableMetadataProvider.class);

  static {
    Map<String, Schema.Type> typeLookup = new HashMap<>();
    typeLookup.put("bigint", Schema.Type.INT64);
    typeLookup.put("bit", Schema.Type.STRING);
    typeLookup.put("boolean", Schema.Type.BOOLEAN);
    typeLookup.put("character", Schema.Type.STRING);
    typeLookup.put("bit varying", Schema.Type.STRING);
    typeLookup.put("character varying", Schema.Type.STRING);
    typeLookup.put("smallint", Schema.Type.INT16);
    typeLookup.put("integer", Schema.Type.INT32);
    typeLookup.put("json", Schema.Type.STRING);
    typeLookup.put("jsonb", Schema.Type.STRING);
    typeLookup.put("real", Schema.Type.FLOAT32);
    typeLookup.put("double precision", Schema.Type.FLOAT64);
    typeLookup.put("text", Schema.Type.STRING);
    typeLookup.put("xml", Schema.Type.STRING);
    typeLookup.put("uuid", Schema.Type.STRING);
    typeLookup.put("pg_lsn", Schema.Type.STRING);
    typeLookup.put("cidr", Schema.Type.STRING);
    typeLookup.put("macaddr", Schema.Type.STRING);
    typeLookup.put("inet", Schema.Type.STRING);
    typeLookup.put("txid_snapshot", Schema.Type.STRING);
    typeLookup.put("bytea", Schema.Type.BYTES);

    TYPE_LOOKUP = ImmutableMap.copyOf(typeLookup);
  }

  public PostgreSqlTableMetadataProvider(PostgreSqlSourceConnectorConfig config, OffsetStorageReader offsetStorageReader) {
    super(config, offsetStorageReader);
  }

  static Schema buildColumnSchema(final String columnName, ResultSet resultSet) throws SQLException {
    boolean nullable = "YES".equalsIgnoreCase(resultSet.getString(2));
    String dataType = resultSet.getString(3);
    int scale = resultSet.getInt(4);

    SchemaBuilder builder;

    if (TYPE_LOOKUP.containsKey(dataType)) {
      Schema.Type type = TYPE_LOOKUP.get(dataType);
      builder = SchemaBuilder.type(type);
    } else {
      if ("numeric".equalsIgnoreCase(dataType)) {
        builder = Decimal.builder(scale);
      } else if ("money".equalsIgnoreCase(dataType)) {
        builder = Decimal.builder(scale);
      } else if ("date".equalsIgnoreCase(dataType)) {
        builder = Date.builder();
      } else if ("time without time zone".equalsIgnoreCase(dataType)) {
        builder = Time.builder();
      } else if ("time with time zone".equalsIgnoreCase(dataType)) {
        builder = Time.builder();
      } else if ("timestamp without time zone".equalsIgnoreCase(dataType)) {
        builder = Timestamp.builder();
      } else if ("timestamp with time zone".equalsIgnoreCase(dataType)) {
        builder = Timestamp.builder();
      } else if ("point".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.pointSchema();
      } else if ("box".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.boxSchema();
      } else if ("circle".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.circleSchema();
      } else if ("lseg".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.lsegSchema();
      } else if ("path".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.pathSchema();
      } else if ("polygon".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.polygonSchema();
      } else if ("line".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.lineSchema();
      } else if ("interval".equalsIgnoreCase(dataType)) {
        builder = PostgreSqlConstants.intervalSchema();
      } else {
        String message = String.format(
            "Could not determine schema for %s: dataType='%s' scale=%d nullable=%s",
            columnName,
            dataType,
            scale,
            nullable
        );
        throw new DataException(message);
      }
    }

    if (nullable) {
      builder.optional();
    }

    builder.parameter(Change.ColumnValue.COLUMN_NAME, columnName);

    return builder.build();
  }

  static Set<String> findKeys(Connection connection, ChangeKey changeKey) throws SQLException {
    Set<String> keys = new LinkedHashSet<>();

    if (log.isTraceEnabled()) {
      log.trace("{}: Querying for primary keys.", changeKey);
    }

    try (PreparedStatement preparedStatement = connection.prepareStatement(PRIMARY_KEY_SQL)) {
//      preparedStatement.setString(1, changeKey.databaseName);
      preparedStatement.setString(1, changeKey.schemaName);
      preparedStatement.setString(2, changeKey.tableName);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          String columnName = resultSet.getString(1);
          keys.add(columnName);
        }
      }
    }

    if (!keys.isEmpty()) {
      if (log.isTraceEnabled()) {
        log.trace("{}: Found keys {}",
            changeKey,
            Joiner.on(", ").join(keys)
        );
      }
      return keys;
    }


    return keys;
  }

  @Override
  public Map<String, Object> startOffset(ChangeKey changeKey) throws SQLException {
    return null;
  }

  @Override
  protected TableMetadata fetchTableMetadata(ChangeKey changeKey) throws SQLException {
    PooledConnection pooledConnection = null;

    PostgreSQLTableMetadata tableMetadata = new PostgreSQLTableMetadata();
    tableMetadata.databaseName = changeKey.databaseName;
    tableMetadata.schemaName = changeKey.schemaName;
    tableMetadata.tableName = changeKey.tableName;

    try {
      pooledConnection = JdbcUtils.openPooledConnection(this.config, changeKey);
      try (PreparedStatement preparedStatement = pooledConnection.getConnection().prepareStatement(COLUMN_SQL)) {
        preparedStatement.setString(1, changeKey.databaseName);
        preparedStatement.setString(2, changeKey.schemaName);
        preparedStatement.setString(3, changeKey.tableName);

        Map<String, Schema> columnSchemas = new LinkedHashMap<>();

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          while (resultSet.next()) {
            final String columnName = resultSet.getString(1);
            Schema schema = buildColumnSchema(columnName, resultSet);
            columnSchemas.put(columnName, schema);
          }
        }

        Preconditions.checkState(!columnSchemas.isEmpty(), "No columns were found for %s", changeKey);
        tableMetadata.columnSchemas = columnSchemas;
        tableMetadata.keyColumns = findKeys(pooledConnection.getConnection(), changeKey);
      }
    } finally {
      JdbcUtils.closeConnection(pooledConnection);
    }

    return tableMetadata;
  }

  class PostgreSQLTableMetadata implements TableMetadata {
    String databaseName;
    String schemaName;
    String tableName;
    Set<String> keyColumns;
    Map<String, Schema> columnSchemas;

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
    public Set<String> keyColumns() {
      return this.keyColumns;
    }

    @Override
    public Map<String, Schema> columnSchemas() {
      return this.columnSchemas;
    }
  }
}
