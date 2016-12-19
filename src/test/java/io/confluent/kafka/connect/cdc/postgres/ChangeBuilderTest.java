package io.confluent.kafka.connect.cdc.postgres;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.cdc.Change;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChangeBuilderTest {
  static final String SLOT_NAME = "testing";
  ChangeBuilder changeBuilder;

  @Before
  public void before() {
    this.changeBuilder = new ChangeBuilder(SLOT_NAME);
  }

  ResultSet mockResultSet(String location, Long xid, String data) throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn(location);
    when(resultSet.getLong(2)).thenReturn(xid);
    when(resultSet.getString(3)).thenReturn(data);
    return resultSet;
  }

  @Test
  public void insert_00() throws SQLException {
    final String OFFSET="0/151CDF0";
    ResultSet input = mockResultSet(OFFSET, 555L, "table public.simple_table: INSERT: user_id[bigint]:1 email_address[character varying]:'hinton@yahoo.com' first_name[character varying]:'Anthony' last_name[character varying]:'Hinton' description[text]:'Pain but example, pain, to. No there great. But physical pleasure?. Expound No which pain of fault. Not has produces take all pleasure.''Has it ever. Man who occur. Who But teachings pain rejects, expound pleasure, occasionally happiness. Of pleasure. Pursues because because.'");
    Change change = this.changeBuilder.build(input);
    assertNotNull("Change should not be null.", change);
    assertEquals("changeType does not match.", Change.ChangeType.INSERT, change.changeType());
    assertEquals("schemaName does not match", "public", change.schemaName());
    assertEquals("table does not match", "simple_table", change.tableName());
    assertNotNull("keyColumns should not be null.", change.keyColumns());
    assertNotNull("valueColumns should not be null.", change.valueColumns());

    final Map<String, Object> expectedSourceOffset = ImmutableMap.of(SLOT_NAME, (Object)OFFSET);
    assertThat("sourceOffset does not match.", change.sourceOffset(), IsEqual.equalTo(expectedSourceOffset));

    final Map<String, Object> expectedSourcePartition = ImmutableMap.of();
    assertThat("sourcePartition does not match.", change.sourcePartition(), IsEqual.equalTo(expectedSourcePartition));

    Map<String, PostgreSQLColumnValue> keyColumns;
  }

}
