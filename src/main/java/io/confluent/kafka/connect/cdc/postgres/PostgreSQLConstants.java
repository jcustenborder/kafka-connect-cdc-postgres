package io.confluent.kafka.connect.cdc.postgres;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

class PostgreSQLConstants {

  public static final String SCHEMA_NAME_POINT = "io.confluent.kafka.connect.cdc.postgres.schema.Point";
  public static final String SCHEMA_NAME_BOX = "io.confluent.kafka.connect.cdc.postgres.schema.Box";
  public static final String SCHEMA_NAME_CIRCLE = "io.confluent.kafka.connect.cdc.postgres.schema.Circle";
  public static final String SCHEMA_NAME_LSEG = "io.confluent.kafka.connect.cdc.postgres.schema.Lseg";
  public static final String SCHEMA_NAME_PATH = "io.confluent.kafka.connect.cdc.postgres.schema.Path";
  public static final String SCHEMA_NAME_LINE = "io.confluent.kafka.connect.cdc.postgres.schema.Line";
  public static final String SCHEMA_NAME_POLYGON = "io.confluent.kafka.connect.cdc.postgres.schema.Polygon";
  public static final String SCHEMA_NAME_INTERVAL = "io.confluent.kafka.connect.cdc.postgres.schema.Interval";

  static SchemaBuilder intervalSchema() {
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_INTERVAL)
        .doc("This implements a class that handles the PostgreSQL interval type")
        .field("years", SchemaBuilder.int32().doc("Returns the years represented by this interval.").build())
        .field("months", SchemaBuilder.int32().doc("Returns the months represented by this interval.").build())
        .field("days", SchemaBuilder.int32().doc("Returns the days represented by this interval.").build())
        .field("hours", SchemaBuilder.int32().doc("Returns the hours represented by this interval.").build())
        .field("minutes", SchemaBuilder.int32().doc("Returns the minutes represented by this interval.").build())
        .field("seconds", SchemaBuilder.int32().doc("Returns the seconds represented by this interval.").build());
  }

  static SchemaBuilder pathSchema() {
    Schema pointSchema = pointSchema();
    Schema pointArraySchema = SchemaBuilder.array(pointSchema).doc("The points defining this path.").build();
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_PATH)
        .field("open", SchemaBuilder.bool().doc("True if the path is open, false if closed").build())
        .field("points", pointArraySchema);
  }

  static SchemaBuilder lineSchema() {
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_LINE)
        .field("a", SchemaBuilder.float64().doc("Coefficient of x.").build())
        .field("b", SchemaBuilder.float64().doc("Coefficient of y.").build())
        .field("c", SchemaBuilder.float64().doc("Constant.").build())
        ;
  }

  static SchemaBuilder lsegSchema() {
    Schema pointSchema = pointSchema();
    Schema pointArraySchema = SchemaBuilder.array(pointSchema).doc("These are the two points.").build();
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_LSEG)
        .field("point", pointArraySchema);
  }

  static SchemaBuilder circleSchema() {
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_CIRCLE)
        .field("center", pointSchema().doc("This is the center point").build())
        .field("radius", SchemaBuilder.float64().doc("This is the radius").build());
  }

  static SchemaBuilder pointSchema() {
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_POINT)
        .field("x", SchemaBuilder.float64().doc("The X coordinate of the point").build())
        .field("y", SchemaBuilder.float64().doc("The y coordinate of the point").build());
  }

  static Struct pointStruct(Schema schema, double x, double y) {
    return new Struct(schema).put("x", x).put("y", y);
  }


  static SchemaBuilder boxSchema() {
    Schema pointSchema = pointSchema();
    Schema pointArraySchema = SchemaBuilder.array(pointSchema).doc("These are the two points.").build();
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_BOX)
        .field("point", pointArraySchema);
  }

  static SchemaBuilder polygonSchema() {
    Schema pointSchema = pointSchema();
    Schema pointArraySchema = SchemaBuilder.array(pointSchema).doc("The points defining the polygon.").build();
    return SchemaBuilder.struct()
        .name(SCHEMA_NAME_POLYGON)
        .field("points", pointArraySchema);
  }
}
