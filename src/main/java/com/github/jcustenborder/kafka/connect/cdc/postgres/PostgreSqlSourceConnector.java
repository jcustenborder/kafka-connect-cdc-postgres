/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.github.jcustenborder.kafka.connect.cdc.CDCSourceConnector;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.google.common.base.Preconditions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Description("The PostgreSql uses the [Logical Decoding](https://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html) " +
    "feature in PostgreSql 9.x. This will allow this connector to work with [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/).")
public class PostgreSqlSourceConnector extends CDCSourceConnector {
  Map<String, String> settings;
  PostgreSqlSourceConnectorConfig config;

  @Override
  public void start(Map<String, String> settings) {
    this.settings = settings;
    this.config = new PostgreSqlSourceConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return PostgreSqlSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int taskCount) {
    Preconditions.checkState(taskCount > 0, "At least one task is required");
    return Arrays.asList(this.settings);
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return PostgreSqlSourceConnectorConfig.config();
  }
}
