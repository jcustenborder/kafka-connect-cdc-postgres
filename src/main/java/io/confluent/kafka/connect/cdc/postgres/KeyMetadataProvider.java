package io.confluent.kafka.connect.cdc.postgres;

import java.sql.SQLException;
import java.util.Set;

public interface KeyMetadataProvider {
  Set<String> keys(String schemaName, String tableName) throws SQLException;
}
