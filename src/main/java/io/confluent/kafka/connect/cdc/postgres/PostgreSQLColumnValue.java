package io.confluent.kafka.connect.cdc.postgres;

import io.confluent.kafka.connect.cdc.ColumnValue;
import org.apache.kafka.connect.data.Schema;

public class PostgreSQLColumnValue implements ColumnValue {
  String columnName;
  Schema schema;
  Object value;

  @Override
  public String columnName() {
    return this.columnName;
  }

  @Override
  public Schema schema() {
    return this.schema;
  }

  @Override
  public Object value() {
    return this.value;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ColumnValue)) {
      return false;
    }

    ColumnValue that = (ColumnValue) obj;
    return
        this.columnName().equals(that.columnName()) &&
            this.schema.type().equals(that.schema().type()) &&
            this.schema.isOptional() == that.schema().isOptional() &&
            this.value() == that.value()
        ;
  }
}
