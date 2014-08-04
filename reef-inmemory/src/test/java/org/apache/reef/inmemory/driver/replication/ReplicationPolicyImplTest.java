package org.apache.reef.inmemory.driver.replication;

import com.google.common.collect.Lists;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.Condition;
import org.apache.reef.inmemory.common.replication.Rule;
import org.apache.reef.inmemory.common.replication.Rules;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test functionality of ReplicationPolicyImpl
 */
public final class ReplicationPolicyImplTest {

  private Action defaultAction;

  @Before
  public void setUp() {
    defaultAction = new Action(1, false);
  }

  /**
   * Test Exact Path matching
   */
  @Test
  public void testExactPath() {
    final String path = "/exact/matching/path";
    final Condition condition = new Condition("path", "exact", path);
    final Action action = new Action(10, false);

    final Rule rule = new Rule("exact-test", Lists.newArrayList(condition), action);
    final Rules rules = new Rules(Lists.newArrayList(rule), defaultAction);

    final ReplicationPolicy policy = new ReplicationPolicyImpl(rules);
    final Action policyAction = policy.getReplicationAction(path, new FileMeta());
    assertEquals(action, policyAction);

    final Action noMatch1 = policy.getReplicationAction("/other" + path, new FileMeta());
    assertEquals(defaultAction, noMatch1);

    final Action noMatch2 = policy.getReplicationAction(path + "/other", new FileMeta());
    assertEquals(defaultAction, noMatch2);
  }

  /**
   * Test Recursive Path matching
   */
  @Test
  public void testRecursivePath() {
    final String path = "/recursive/dir";
    final Condition condition = new Condition("path", "recursive", path);
    final Action action = new Action(10, false);

    final Rule rule = new Rule("recursive-test", Lists.newArrayList(condition), action);
    final Rules rules = new Rules(Lists.newArrayList(rule), defaultAction);

    final ReplicationPolicy policy = new ReplicationPolicyImpl(rules);
    final Action policyAction1 = policy.getReplicationAction(path, new FileMeta());
    assertEquals(action, policyAction1);
    final Action policyAction2 = policy.getReplicationAction(path + "/file", new FileMeta());
    assertEquals(action, policyAction2);
    final Action policyAction3 = policy.getReplicationAction(path + "/anotherDir/file", new FileMeta());
    assertEquals(action, policyAction3);

    final Action noMatch1 = policy.getReplicationAction("/other" + path, new FileMeta());
    assertEquals(defaultAction, noMatch1);

    final Action noMatch2 = policy.getReplicationAction(path + "-sameprefix", new FileMeta());
    assertEquals(defaultAction, noMatch2);
  }

  /**
   * Test human readable size (e.g., 128K, 56m) matching
   */
  @Test
  public void testSizeHumanReadable() {
    final String path = "/recursive/dir";
    final Condition condition1 = new Condition("path", "recursive", path);
    final Condition condition2 = new Condition("size", "lt", "128M");
    final Action action = new Action(10, true);

    final Rule rule = new Rule("recursive-dir", Lists.newArrayList(condition1, condition2), action);
    final Rules rules = new Rules(Lists.newArrayList(rule), defaultAction);

    final ReplicationPolicy policy = new ReplicationPolicyImpl(rules);

    doTest128M(policy, path, action);
  }

  /**
   * Test size in bytes matching
   */
  @Test
  public void testSizeRaw() {
    final String path = "/recursive/dir";
    final Condition condition1 = new Condition("path", "recursive", path);
    final Condition condition2 = new Condition("size", "lt", Integer.toString(128 * 1024 * 1024));
    final Action action = new Action(10, true);

    final Rule rule = new Rule("recursive-dir", Lists.newArrayList(condition1, condition2), action);
    final Rules rules = new Rules(Lists.newArrayList(rule), defaultAction);

    final ReplicationPolicy policy = new ReplicationPolicyImpl(rules);

    doTest128M(policy, path, action);
  }

  /**
   * Test that matching on a bad condition returns false
   */
  @Test
  public void testSizeBad() {
    final String path = "/recursive/dir";
    final Condition condition1 = new Condition("path", "recursive", path);
    final Condition condition2 = new Condition("size", "lt", "badsize");
    final Action action = new Action(10, true);

    final Rule rule = new Rule("recursive-dir", Lists.newArrayList(condition1, condition2), action);
    final Rules rules = new Rules(Lists.newArrayList(rule), defaultAction);

    final ReplicationPolicy policy = new ReplicationPolicyImpl(rules);

    final FileMeta metadata1 = new FileMeta();
    metadata1.setFileSize(1 * 1024 * 1024);
    final Action policyAction1 = policy.getReplicationAction(path + "/file1", metadata1);
    assertEquals(defaultAction, policyAction1);
  }

  private void doTest128M(ReplicationPolicy policy, String path, Action action) {
    final FileMeta metadata1 = new FileMeta();
    metadata1.setFileSize(1 * 1024 * 1024);
    final Action policyAction1 = policy.getReplicationAction(path + "/file1", metadata1);
    assertEquals(action, policyAction1);

    final FileMeta metadata2 = new FileMeta();
    metadata2.setFileSize(128 * 1024 * 1024 - 1);
    final Action policyAction2 = policy.getReplicationAction(path + "/file2", metadata2);
    assertEquals(action, policyAction2);

    final FileMeta noMatchMetadata1 = new FileMeta();
    noMatchMetadata1.setFileSize(128 * 1024 * 1024);
    final Action noMatch1 = policy.getReplicationAction(path + "/file1", noMatchMetadata1);
    assertEquals(defaultAction, noMatch1);

    final FileMeta noMatchMetadata2 = new FileMeta();
    noMatchMetadata2.setFileSize(256 * 1024 * 1024);
    final Action noMatch2 = policy.getReplicationAction(path + "/file2", noMatchMetadata2);
    assertEquals(defaultAction, noMatch2);

    final FileMeta noMatchMetadata3 = new FileMeta();
    noMatchMetadata3.setFileSize(1 * 1024 * 1024);
    final Action noMatch3 = policy.getReplicationAction("/bad/path", noMatchMetadata3);
    assertEquals(defaultAction, noMatch3);
  }
}
