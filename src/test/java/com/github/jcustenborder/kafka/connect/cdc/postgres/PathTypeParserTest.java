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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class PathTypeParserTest {
  Parsers.PathTypeParser parser;
  Schema schema;

  @BeforeEach
  public void before() {
    this.schema = PostgreSqlConstants.pathSchema();
    this.parser = new Parsers.PathTypeParser();
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    List<TestCase> testCases = Arrays.asList(
        new TestCase("((0,0),(1,1),(2,0))", 0D, 0D, 1D, 1D, 2D, 0D),
        new TestCase("((0,0),(-1,1),(-2,0))", 0D, 0D, -1D, 1D, -2D, 0D)
    );

    return testCases.stream().map(data -> dynamicTest(data.input, () -> parse(data)));
  }

  void parse(TestCase testCase) {
    final Object result = parser.parseString(testCase.input, this.schema);
    assertNotNull(result, "result should not be null.");
    assertEquals(this.parser.expectedClass(), result.getClass(), "Class is not as expected");
    final Struct actual = (Struct) result;
    actual.validate();
    assertTrue(testCase.points.length % 2 == 0);
    List<Struct> actualPoints = actual.getArray("points");

    int index = 0;
    for (int i = 0; i < testCase.points.length; i += 2) {
      final double x = testCase.points[i];
      final double y = testCase.points[i + 1];
      Struct actualPoint = actualPoints.get(index);
      assertEquals(x, actualPoint.get("x"), String.format("points[%d].x does not match", index));
      assertEquals(y, actualPoint.get("y"), String.format("points[%d].y does not match", index));
      index++;
    }
  }

  class TestCase {
    final String input;
    final double[] points;

    private TestCase(String input, double... points) {
      this.input = input;
      this.points = points;
    }
  }
}
