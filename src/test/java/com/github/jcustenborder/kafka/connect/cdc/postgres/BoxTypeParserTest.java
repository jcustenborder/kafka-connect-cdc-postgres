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

public class BoxTypeParserTest {
  Parsers.BoxTypeParser parser;
  Schema schema;

  @BeforeEach
  public void before() {
    this.schema = PostgreSqlConstants.boxSchema();
    this.parser = new Parsers.BoxTypeParser();
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    List<TestCase> testCases = Arrays.asList(
        new TestCase("(1,1),(0,0)", 1D, 1D, 0D, 0D),
        new TestCase("(1,1),(-2,-2)", 1D, 1D, -2D, -2D)
    );

    return testCases.stream().map(data -> dynamicTest(data.input, () -> parse(data)));
  }

  void parse(TestCase testCase) {
    final Object result = parser.parseString(testCase.input, this.schema);
    assertNotNull(result, "result should not be null.");
    assertEquals(this.parser.expectedClass(), result.getClass(), "Class is not as expected");
    final Struct actual = (Struct) result;
    actual.validate();
    List<Struct> values = (List<Struct>) actual.get(Parsers.BoxTypeParser.FIELD_BOX_POINT);
    assertFalse(values.isEmpty(), "values should not be empty.");
    assertEquals(2, values.size(), "size does not match.");
    assertEquals(testCase.x1, values.get(0).get("x"), "x1 does not match");
    assertEquals(testCase.y1, values.get(0).get("y"), "y1 does not match");
    assertEquals(testCase.x2, values.get(1).get("x"), "x1 does not match");
    assertEquals(testCase.y2, values.get(1).get("y"), "y1 does not match");
  }

  class TestCase {
    final String input;
    final double x1;
    final double y1;
    final double x2;
    final double y2;

    private TestCase(String input, double x1, double y1, double x2, double y2) {
      this.input = input;
      this.x1 = x1;
      this.y1 = y1;
      this.x2 = x2;
      this.y2 = y2;
    }
  }
}
