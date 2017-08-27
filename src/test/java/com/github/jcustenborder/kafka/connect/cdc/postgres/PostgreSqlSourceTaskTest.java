/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres;

import com.github.jcustenborder.kafka.connect.cdc.Integration;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerCompose;
import com.github.jcustenborder.kafka.connect.cdc.postgres.docker.PostgreSqlClusterHealthCheck;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Disabled;

@Disabled
@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSqlClusterHealthCheck.class)
public class PostgreSqlSourceTaskTest extends PostgreSqlTest {
//  PostgreSqlSourceTask task;
//
//  @BeforeEach
//  public void start(@DockerFormatString(container = PostgreSqlTestConstants.CONTAINER_NAME, port = PostgreSqlTestConstants.PORT, format = PostgreSqlTestConstants.JDBC_URL_FORMAT) String jdbcUrl) {
//    Map<String, String> settings = ImmutableMap.of(
//        PostgreSqlSourceConnectorConfig.JDBC_URL_CONF, jdbcUrl,
//        PostgreSqlSourceConnectorConfig.JDBC_PASSWORD_CONF, PostgreSqlTestConstants.PASSWORD,
//        PostgreSqlSourceConnectorConfig.JDBC_USERNAME_CONF, PostgreSqlTestConstants.USERNAME,
//        PostgreSqlSourceConnectorConfig.POSTGRES_REPLICATION_SLOT_NAME_CONF, "testing"
//    );
//
//    this.task = new PostgreSqlSourceTask();
//    this.task.start(settings);
//  }
//
//  @Test
//  public void foo() {
//
//  }
//
//  @AfterEach
//  public void stop() {
//    this.task.stop();
//  }
}
