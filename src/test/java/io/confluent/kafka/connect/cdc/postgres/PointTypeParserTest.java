package io.confluent.kafka.connect.cdc.postgres;

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

public class PointTypeParserTest {
  Parsers.PointTypeParser parser;
  Schema schema;

  @BeforeEach
  public void before() {
    this.schema = PostgreSqlConstants.pointSchema();
    this.parser = new Parsers.PointTypeParser();
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    List<TestCase> testCases = Arrays.asList(
        new TestCase("(30.267199999999999,97.7430999999999983)", 30.267199999999999D, 97.7430999999999983D)
    );

    return testCases.stream().map(data -> dynamicTest(data.input, () -> parse(data)));
  }

  void parse(TestCase testCase) {
    final Object result = parser.parseString(testCase.input, this.schema);
    assertNotNull(result, "result should not be null.");
    assertEquals(this.parser.expectedClass(), result.getClass(), "Class is not as expected");
    final Struct actual = (Struct) result;
    actual.validate();
    assertEquals(testCase.x, actual.get("x"), "x does not match");
    assertEquals(testCase.y, actual.get("y"), "y does not match");
  }

  class TestCase {
    final String input;
    final double x;
    final double y;

    private TestCase(String input, double x, double y) {
      this.input = input;
      this.x = x;
      this.y = y;
    }
  }
}
