package org.apache.reef.inmemory.common.replication;

import org.apache.avro.AvroTypeException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test reading and writing of well-defined and ill-defined replication policies
 */
public final class AvroReplicationSerializerTest {

  /**
   * Check Rules are read from a file correctly.
   */
  @Test
  public void testRulesRead() throws IOException {
    URL url = this.getClass().getResource("/replication.json");
    final File json = new File(url.getFile());
    final Rules rules = AvroReplicationSerializer.fromStream(new FileInputStream(json));

    assertEquals("path", rules.getRules().get(0).getConditions().get(0).getType().toString());
  }

  /**
   * Check Rules are written to a stream without Exception.
   */
  @Test
  public void testRulesWrite() throws IOException {
    final Condition conditionA1 = new Condition("path", "recursive", "/daily/");
    final Condition conditionA2 = new Condition("size", "lt", "128M");

    final Condition conditionB1 = new Condition("path", "recursive", "/hourly/");
    final Condition conditionB2 = new Condition("size", "gt", "12K");

    final List<Condition> conditionListA = new LinkedList<>();
    conditionListA.add(conditionA1);
    conditionListA.add(conditionA2);

    final List<Condition> conditionListB = new LinkedList<>();
    conditionListB.add(conditionB1);
    conditionListB.add(conditionB2);

    final Action actionA = new Action(5, true);
    final Action actionB = new Action(-1, false);

    final Rule ruleA = new Rule("conditionA", conditionListA, actionA);
    final Rule ruleB = new Rule("conditionB", conditionListB, actionB);
    final Action defaultAction = new Action(2, false);

    final List<Rule> ruleList = new LinkedList<>();
    ruleList.add(ruleA);
    ruleList.add(ruleB);

    final Rules rules = new Rules(ruleList, defaultAction);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    AvroReplicationSerializer.toStream(rules, out);

    System.out.println(out.toString());
  }

  /**
   * Check that a rule with only a default is read without exception
   */
  @Test
  public void testBlankRules() throws IOException {
    final String blankRules = "{\"rules\":[],\"default\":{\"factor\":1,\"pin\":false}}";
    final Rules rules = AvroReplicationSerializer.fromString(blankRules);
  }

  /**
   * Check that a rule without a default raises an exception
   */
  @Test(expected = AvroTypeException.class)
  public void testBlankDefault() throws IOException {
    final String blankDefault = "{\"rules\":[],\"default\":{}}";
    final Rules rules = AvroReplicationSerializer.fromString(blankDefault);
  }
}
