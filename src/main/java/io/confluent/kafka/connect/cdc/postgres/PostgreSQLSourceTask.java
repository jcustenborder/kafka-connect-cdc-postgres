package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.CDCSourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PostgreSQLSourceTask extends CDCSourceTask<PostgreSQLSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(PostgreSQLSourceTask.class);

  @Override
  protected PostgreSQLSourceConnectorConfig getConfig(Map<String, String> map) {
    return new PostgreSQLSourceConnectorConfig(map);
  }

  @Override
  public void start(Map<String, String> map) {
    super.start(map);
  }

  @Override
  public void stop() {


  }


}
