package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.base.Strings;
import io.confluent.kafka.connect.cdc.ChangeKey;
import io.confluent.kafka.connect.cdc.ConnectionKey;
import io.confluent.kafka.connect.cdc.ConnectionPoolDataSourceFactory;
import org.postgresql.ds.PGConnectionPoolDataSource;

import javax.sql.ConnectionPoolDataSource;
import java.sql.SQLException;

class PostgreSqlConnectionPoolDataSourceFactory implements ConnectionPoolDataSourceFactory {

  final PostgreSqlSourceConnectorConfig config;

  PostgreSqlConnectionPoolDataSourceFactory(PostgreSqlSourceConnectorConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPoolDataSource connectionPool(ConnectionKey connectionKey) throws SQLException {
    PGConnectionPoolDataSource poolingDataSource = new PGConnectionPoolDataSource();
    poolingDataSource.setServerName(this.config.serverName);
    poolingDataSource.setPortNumber(this.config.serverPort);
    poolingDataSource.setUser(this.config.jdbcUsername);
    poolingDataSource.setPassword(this.config.jdbcPassword);

    if (Strings.isNullOrEmpty(connectionKey.databaseName)) {
      poolingDataSource.setDatabaseName(this.config.initialDatabase);
    } else {
      poolingDataSource.setDatabaseName(connectionKey.databaseName);
    }
    return poolingDataSource;
  }

  @Override
  public ConnectionKey connectionKey(ChangeKey changeKey) {
    return ConnectionKey.of(this.config.serverName, this.config.serverPort, this.config.jdbcUsername, changeKey.databaseName);
  }
}
