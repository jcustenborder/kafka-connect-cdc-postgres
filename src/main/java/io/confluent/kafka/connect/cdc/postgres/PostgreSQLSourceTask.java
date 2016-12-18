package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.collect.Sets;
import io.confluent.kafka.connect.cdc.CDCSourceTask;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class PostgreSQLSourceTask extends CDCSourceTask<PostgreSQLSourceConnectorConfig> implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(PostgreSQLSourceTask.class);

  @Override
  protected PostgreSQLSourceConnectorConfig getConfig(Map<String, String> map) {
    return new PostgreSQLSourceConnectorConfig(map);
  }

  Connection connection;
  String slot;


  @Override
  public void start(Map<String, String> map) {
    super.start(map);

    this.connection = Utils.openConnection(this.config);
    this.slot = this.config.replicationSlotNames.get(0);
    createLogicalReplicationSlot();
  }

  void createLogicalReplicationSlot() {
    final String SQL = "SELECT 'init' FROM pg_create_logical_replication_slot(?, ?)";
    try (PreparedStatement statement = this.connection.prepareStatement(SQL)) {
      statement.setString(1, this.slot);
      statement.setString(2, "test_decoding");

      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {

        }
      }


    } catch (SQLException ex) {
      throw new DataException("Exception thrown", ex);
    }

  }


  @Override
  public void stop() {
    Utils.closeConnection(this.connection);
  }

  @Override
  public void run() {
    while (true) {
      try {
        queryChanges();
      } catch (Exception ex) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown");
        }
      }
    }
  }

  void queryChanges() throws SQLException, IOException {
    final String SQL = "SELECT * FROM pg_logical_slot_get_changes(?, ?, ?, 'skip-empty-xacts', '1', 'force-binary', '0', 'include-timestamp', '0', 'include-xids', '0')";
    try (PreparedStatement statement = this.connection.prepareStatement(SQL)) {
      statement.setString(1, this.slot);
      statement.setObject(2, null);
      statement.setInt(3, 512);//Number of changes to stream

      try (ResultSet results = statement.executeQuery()) {
        int columns = results.getMetaData().getColumnCount();
        if (log.isInfoEnabled()) {
          log.info("Found {} columns", columns);
        }
        while (results.next()) {
          String location = results.getString(1);
          Long xid = results.getLong(2);
          String data = results.getString(3);

          if (log.isInfoEnabled()) {
            log.info("location='{}' xid='{}' data {}", location, xid, data);
          }
        }
      }
    }
  }

}
