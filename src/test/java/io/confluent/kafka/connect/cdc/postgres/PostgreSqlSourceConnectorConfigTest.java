package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.Test;

public class PostgreSqlSourceConnectorConfigTest {

  @Test
  public void doc() {
    System.out.println(MarkdownFormatter.toMarkdown(PostgreSqlSourceConnectorConfig.config()));
  }

}
