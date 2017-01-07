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

public class PolygonTypeParserTest {
  Parsers.PolygonTypeParser parser;
  Schema schema;

  @BeforeEach
  public void before() {
    this.schema = PostgreSqlConstants.polygonSchema();
    this.parser = new Parsers.PolygonTypeParser();
  }

  @TestFactory
  public Stream<DynamicTest> parse() {
    List<TestCase> testCases = Arrays.asList(
        new TestCase("((0,0),(1,1),(2,0))", 0D, 0D, 1D, 1D, 2D, 0D)
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
