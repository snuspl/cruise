package org.apache.reef.inmemory.common.replication;

import org.apache.avro.AvroTypeException;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test reading and writing of well-defined and ill-defined replication policies
 */
public final class AvroReadWriteTest {

  private static final String JSON_CHARSET = "UTF-8";

  @Test
  public void testRulesRead() throws IOException {
    URL url = this.getClass().getResource("/replication.json");
    final File json = new File(url.getFile());

    final DatumReader<Rules> reader = new SpecificDatumReader<>(Rules.class);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Rules.getClassSchema(),
            new FileInputStream(json));
    final Rules rules = reader.read(null, decoder);
  }

  @Test
  public void testRulesWrite() {
    final Condition conditionA1 = new Condition("size", "lt", "128M");
    final Condition conditionA2 = new Condition("path", "include", "/daily/*/*");

    final Condition conditionB1 = new Condition("path", "include", "/hourly/*");
    final Condition conditionB2 = new Condition("path", "exclude", "/hourly/archive*");

    final List<Condition> conditionListA = new LinkedList<>();
    conditionListA.add(conditionA1);
    conditionListA.add(conditionA2);

    final List<Condition> conditionListB = new LinkedList<>();
    conditionListB.add(conditionB1);
    conditionListB.add(conditionB2);

    final Rule ruleA = new Rule("conditionA", conditionListA, 5, true);
    final Rule ruleB = new Rule("conditionB", conditionListB, -1, false);
    final DefaultRule defaultRule = new DefaultRule(2, false);

    final List<Rule> ruleList = new LinkedList<>();
    ruleList.add(ruleA);
    ruleList.add(ruleB);

    final Rules rules = new Rules(ruleList, defaultRule);

    final DatumWriter<Rules> rulesWriter = new SpecificDatumWriter<>(Rules.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(Rules.SCHEMA$, out);
      rulesWriter.write(rules, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConditionWrite() {
    final Condition writeCondition = new Condition("size", "lt", "128M");

    final DatumWriter<Condition> conditionDatumWriter = new SpecificDatumWriter<>(Condition.class);
    final String result;
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(Condition.SCHEMA$, out);
      conditionDatumWriter.write(writeCondition, encoder);
      encoder.flush();
      out.flush();
      result = out.toString(JSON_CHARSET);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final DatumReader<Condition> conditionDatumReader = new SpecificDatumReader<>(Condition.class);
    try {
      final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Condition.getClassSchema(), result);
      final Condition readCondition = conditionDatumReader.read(null, decoder);
      assertEquals(writeCondition, readCondition);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expected = AvroTypeException.class)
  public void testWithoutDefault() throws IOException {
    final String missingOperand = "{\"type\":\"size\",\"operator\":\"lt\"}";
    final DatumReader<Condition> conditionDatumReader = new SpecificDatumReader<>(Condition.class);
    final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(Condition.getClassSchema(), missingOperand);
    final Condition readCondition = conditionDatumReader.read(null, decoder);
  }
}
