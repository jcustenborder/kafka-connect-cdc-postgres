package io.confluent.kafka.connect.cdc.postgres;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);
  public static Connection openConnection(PostgreSQLSourceConnectorConfig config) {
    try {
      if(log.isInfoEnabled()) {
        log.info("Connecting to {}", config.jdbcUrl);
      }
      return DriverManager.getConnection(config.jdbcUrl, config.jdbcUsername, config.jdbcPassword);
    } catch (SQLException ex) {
      throw new DataException("Exception thrown while connecting to postgres.", ex);
    }
  }

  public static void closeConnection(Connection connection) {
    try{
      if(log.isInfoEnabled()){
        log.info("closing...");
      }
      connection.close();
    } catch (Exception ex) {
      log.error("Exception thrown while closing connection", ex);
    }
  }
}
