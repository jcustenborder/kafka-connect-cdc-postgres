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

import com.github.jcustenborder.kafka.connect.cdc.postgres.Parsers;
import com.github.jcustenborder.kafka.connect.cdc.postgres.PostgreSqlConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class CircleTypeParserTest {
  Parsers.CircleTypeParser parser;
  Schema schema;

  @BeforeEach
  public void before() {
    this.schema = PostgreSqlConstants.circleSchema();
    this.parser = new Parsers.CircleTypeParser();
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    List<TestCase> testCases = Arrays.asList(
        new TestCase("<(-6,-6),2>", -6D, -6D, 2D)
    );

    return testCases.stream().map(data -> dynamicTest(data.input, () -> parse(data)));
  }

  void parse(TestCase testCase) {
    final Object result = parser.parseString(testCase.input, this.schema);
    assertNotNull(result, "result should not be null.");
    assertEquals(this.parser.expectedClass(), result.getClass(), "Class is not as expected");
    final Struct actual = (Struct) result;
    actual.validate();
    Struct center = (Struct) actual.get("center");
    assertNotNull(center, "center should not be null.");
    assertEquals(testCase.x, center.get("x"), "center.x does not match");
    assertEquals(testCase.y, center.get("y"), "center.y does not match");
    assertEquals(testCase.radius, actual.get("radius"), "radius does not match");
  }

  class TestCase {
    final String input;
    final double x;
    final double y;
    final double radius;

    private TestCase(String input, double x, double y, double radius) {
      this.input = input;
      this.x = x;
      this.y = y;
      this.radius = radius;
    }
  }
}
