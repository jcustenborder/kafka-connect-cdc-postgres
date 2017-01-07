package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.util.concurrent.Service;
import io.confluent.kafka.connect.cdc.BaseServiceTask;
import io.confluent.kafka.connect.cdc.ChangeWriter;
import io.confluent.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

class PostgreSqlSourceTask extends BaseServiceTask<PostgreSqlSourceConnectorConfig> {
  ChangeWriter changeWriter;
  Time time = new SystemTime();
  TableMetadataProvider tableMetadataProvider;

  @Override
  protected Service service(ChangeWriter changeWriter, OffsetStorageReader offsetStorageReader) {
    this.changeWriter = changeWriter;
    this.tableMetadataProvider = new PostgreSqlTableMetadataProvider(this.config, offsetStorageReader);
    return new QueryService(this.time, this.tableMetadataProvider, this.config, this.changeWriter);
  }

  @Override
  protected PostgreSqlSourceConnectorConfig getConfig(Map<String, String> map) {
    return new PostgreSqlSourceConnectorConfig(map);
  }
}
