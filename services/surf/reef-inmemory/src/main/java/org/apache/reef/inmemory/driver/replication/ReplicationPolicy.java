package org.apache.reef.inmemory.driver.replication;

import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.Rules;

import java.io.IOException;

/**
 * The replication policy
 * returns a replication Action with the number of replicas and whether the file should be pinned,
 * based on the supplied path and its metadata. In case of writing data into cache,
 * the Action includes two more variables - number of replication and synchronization method to the base FS
 */
public interface ReplicationPolicy {
  /**
   * Return the replication Action for the supplied path and its metadata.
   * When retrieving the factor, the Action should first be checked whether the factor should be set to broadcast
   * using isBroadcast(Action)
   */
  Action getReplicationAction(String path, FileMeta metadata);

  /**
   * Returns whether the Action prescribes broadcast; if not, the factor should be used.
   */
  boolean isBroadcast(Action action);

  /**
   * Get the currently set rules.
   */
  Rules getRules();

  /**
   * Set the rules for this replication policy.
   */
  void setRules(Rules rules);

  /**
   * Set the rules for this replication policy with a JSON String.
  */
  void setRules(String rulesString) throws IOException;
}