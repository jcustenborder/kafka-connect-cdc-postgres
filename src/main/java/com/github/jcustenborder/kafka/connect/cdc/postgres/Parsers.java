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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.connect.utils.data.type.TypeParser;
import com.google.common.base.Preconditions;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parsers {
  static final Pattern POINT_PATTERN = Pattern.compile("^\\((?<x>[\\d\\.-]+)\\s*,\\s*(?<y>[\\d\\.-]+)\\)$");
  static final Pattern POINT_PARTIAL_PATTERN = Pattern.compile("\\((?<x>[\\d\\.-]+)\\s*,\\s*(?<y>[\\d\\.-]+)\\)");
  private static final Logger log = LoggerFactory.getLogger(Parsers.class);

  static void checkSchemaName(Schema schema, String name) {
    Preconditions.checkState(name.equals(schema.name()), "expected '%s' but received schema.name('%s').", name, schema.name());
  }

  static Matcher match(String input, Schema schema, String schemaName, Pattern pattern) {
    checkSchemaName(schema, schemaName);
    Matcher matcher = pattern.matcher(input);
    Preconditions.checkState(matcher.matches(), "'%s' does not match '%s'", input, pattern.pattern());
    return matcher;
  }

  private static void findPoints(Matcher matcher, Schema pointSchema, List<Struct> points) {
    while (matcher.find()) {
      double x = Double.parseDouble(matcher.group("x"));
      double y = Double.parseDouble(matcher.group("y"));
      Struct point = PostgreSqlConstants.pointStruct(pointSchema, x, y);
      points.add(point);
    }
  }

  public static class PointTypeParser implements TypeParser {


    @Override
    public Object parseString(String s, Schema schema) {
      Matcher matcher = match(s, schema, PostgreSqlConstants.SCHEMA_NAME_POINT, POINT_PATTERN);
      double x = Double.parseDouble(matcher.group("x"));
      double y = Double.parseDouble(matcher.group("y"));
      Struct struct = new Struct(schema);
      struct.put("x", x);
      struct.put("y", y);
      return struct;
    }

    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }

  public static class CircleTypeParser implements TypeParser {
    final Pattern pattern = Pattern.compile("^<" + POINT_PARTIAL_PATTERN.pattern() + "," + "(?<radius>[\\d\\.]+)>$");

    @Override
    public Object parseString(String s, Schema schema) {
      Matcher matcher = match(s, schema, PostgreSqlConstants.SCHEMA_NAME_CIRCLE, pattern);
      double x = Double.parseDouble(matcher.group("x"));
      double y = Double.parseDouble(matcher.group("y"));
      double radius = Double.parseDouble(matcher.group("radius"));
      Schema pointSchema = schema.field("center").schema();
      Struct pointStruct = new Struct(pointSchema);
      pointStruct.put("x", x);
      pointStruct.put("y", y);
      Struct struct = new Struct(schema);
      struct.put("center", pointStruct);
      struct.put("radius", radius);
      return struct;
    }


    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }

  public static class BoxTypeParser implements TypeParser {
    public static final String FIELD_BOX_POINT = "point";

    @Override
    public Object parseString(String s, Schema schema) {
      checkSchemaName(schema, PostgreSqlConstants.SCHEMA_NAME_BOX);
      Matcher matcher = POINT_PARTIAL_PATTERN.matcher(s);
      Schema pointSchema = schema.field(FIELD_BOX_POINT).schema().valueSchema();
      List<Struct> points = new ArrayList<>();
      findPoints(matcher, pointSchema, points);
      Struct result = new Struct(schema);
      result.put(FIELD_BOX_POINT, points);
      return result;
    }

    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }

  public static class PolygonTypeParser implements TypeParser {
    final Pattern pattern = Pattern.compile("\\((?<x>[\\d\\.]+),(?<y>[\\d\\.]+)\\)");


    @Override
    public Object parseString(String s, Schema schema) {
      checkSchemaName(schema, PostgreSqlConstants.SCHEMA_NAME_POLYGON);
      String input = s.substring(1, s.length() - 1);
      Matcher matcher = pattern.matcher(input);

      Schema pointSchema = schema.field("points").schema().valueSchema();
      List<Struct> points = new ArrayList<>(10);
      findPoints(matcher, pointSchema, points);

      Struct result = new Struct(schema);
      result.put("points", points);
      return result;
    }

    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }

  public static class PathTypeParser implements TypeParser {
    @Override
    public Object parseString(String s, Schema schema) {
      checkSchemaName(schema, PostgreSqlConstants.SCHEMA_NAME_PATH);
      String input = s.substring(1, s.length() - 1);
      Matcher matcher = POINT_PARTIAL_PATTERN.matcher(input);

      Schema pointSchema = schema.field("points").schema().valueSchema();
      List<Struct> points = new ArrayList<>(10);
      findPoints(matcher, pointSchema, points);

      Struct result = new Struct(schema);
      result.put("open", true); //TODO: This doesn't feel right.
      result.put("points", points);
      return result;
    }

    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }

  public static class LsegTypeParser implements TypeParser {
    @Override
    public Object parseString(String s, Schema schema) {
      checkSchemaName(schema, PostgreSqlConstants.SCHEMA_NAME_LSEG);
      String input = s.substring(1, s.length() - 1);
      Matcher matcher = POINT_PARTIAL_PATTERN.matcher(input);

      Schema pointSchema = schema.field("point").schema().valueSchema();
      List<Struct> points = new ArrayList<>(10);
      findPoints(matcher, pointSchema, points);

      Struct result = new Struct(schema);
      result.put("point", points);
      return result;
    }

    @Override
    public Class<?> expectedClass() {
      return Struct.class;
    }

    @Override
    public Object parseJsonNode(JsonNode jsonNode, Schema schema) {
      return null;
    }
  }
}