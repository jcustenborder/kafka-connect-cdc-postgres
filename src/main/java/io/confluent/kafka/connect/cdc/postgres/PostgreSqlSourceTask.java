package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.CDCSourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PostgreSqlSourceTask extends CDCSourceTask<PostgreSqlSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(PostgreSqlSourceTask.class);

  @Override
  protected PostgreSqlSourceConnectorConfig getConfig(Map<String, String> map) {
    return new PostgreSqlSourceConnectorConfig(map);
  }

  @Override
  public void start(Map<String, String> map) {
    super.start(map);
  }

  @Override
  public void stop() {


  }


}
