package io.confluent.kafka.connect.cdc.postgres;

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
