package io.confluent.kafka.connect.cdc.postgres.docker.healthcheck;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.Attempt;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import io.confluent.kafka.connect.cdc.postgres.docker.DockerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLHealthCheck implements HealthCheck<Container> {
  private static final Logger log = LoggerFactory.getLogger(PostgreSQLHealthCheck.class);
  final String username;
  final String password;

  public PostgreSQLHealthCheck(String username, String password) {
    this.username = username;
    this.password = password;
  }

  @Override
  public SuccessOrFailure isHealthy(Container container) {

    final String jdbcUrl = DockerUtils.jdbcUrl(container);

    if (log.isInfoEnabled()) {
      log.info("Attempting to authenticate to {} with user {}.", jdbcUrl, this.username);
    }

    return SuccessOrFailure.onResultOf(
        new Attempt() {
          @Override
          public boolean attempt() throws Exception {
            try (Connection connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
            )) {
              return true;
            } catch (Exception ex) {
              if(log.isDebugEnabled()) {
                log.debug("Exception thrown", ex);
              }

              Thread.sleep(2000);
              return false;
            }

          }
        }
    );
  }
}
