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


import com.google.common.util.concurrent.ServiceManager;
import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.ChangeWriter;
import com.github.jcustenborder.kafka.connect.cdc.Integration;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import com.github.jcustenborder.kafka.connect.cdc.docker.DockerCompose;
import com.github.jcustenborder.kafka.connect.cdc.postgres.docker.PostgreSqlClusterHealthCheck;
import com.github.jcustenborder.kafka.connect.cdc.postgres.docker.PostgreSqlSettings;
import com.github.jcustenborder.kafka.connect.cdc.postgres.docker.PostgreSqlSettingsExtension;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@Category(Integration.class)
@DockerCompose(dockerComposePath = PostgreSqlTestConstants.DOCKER_COMPOSE_FILE, clusterHealthCheck = PostgreSqlClusterHealthCheck.class)
@ExtendWith(PostgreSqlSettingsExtension.class)
public class QueryServiceTest extends PostgreSqlTest {
  PostgreSqlSourceConnectorConfig config;
  QueryService queryService;
  TableMetadataProvider tableMetadataProvider;
  ChangeWriter changeWriter;
  ServiceManager serviceManager;
  CountDownLatch countDownLatch = new CountDownLatch(30);

  @BeforeEach
  public void settings(@PostgreSqlSettings Map<String, String> settings) throws TimeoutException {
    this.config = new PostgreSqlSourceConnectorConfig(settings);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    this.tableMetadataProvider = new PostgreSqlTableMetadataProvider(this.config, offsetStorageReader);
    this.changeWriter = mock(ChangeWriter.class);
    doAnswer(invocationOnMock -> {
      countDownLatch.countDown();
      return null;
    }).when(this.changeWriter).addChange(any(Change.class));
    this.queryService = new QueryService(new SystemTime(), this.tableMetadataProvider, this.config, changeWriter);
    this.serviceManager = new ServiceManager(Arrays.asList(this.queryService));
    this.serviceManager.startAsync();
    this.serviceManager.awaitHealthy(30, TimeUnit.SECONDS);
  }

  @Test
  public void waitForChanges() throws SQLException, InterruptedException {
    countDownLatch.await(10, TimeUnit.SECONDS);
    verify(this.changeWriter, atLeastOnce()).addChange(any(Change.class));
  }

  @AfterEach
  public void after() throws TimeoutException {
    this.serviceManager.stopAsync();
    this.serviceManager.awaitStopped(30, TimeUnit.SECONDS);
  }

}
