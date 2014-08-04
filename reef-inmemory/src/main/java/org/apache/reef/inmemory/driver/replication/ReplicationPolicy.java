package org.apache.reef.inmemory.driver.replication;

import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.Rules;

import java.io.IOException;

/**
 * The replication policy
 * returns a replication factor based on the supplied path and its metadata.
 */
public interface ReplicationPolicy {
  /**
   * Return the replication Action for the supplied path and its metadata.
   * When retrieving the factor, the Action should first be checked whether it prescribes broadcast
   * using isBroadcast(Action)
   */
  public Action getReplicationAction(String path, FileMeta metadata);

  /**
   * Returns whether the Action prescribes broadcast; if not, the factor should be used.
   * TODO: this interface should be part of Action, but can't add it to Avro-generated class. What to do?
   */
  public boolean isBroadcast(Action action);

  /**
   * Set the rules for this replication policy
   */
  public void setRules(Rules rules);

  /**
   * Set the rules for this replication policy with a JSON String
  */
  public void setRules(String rulesString) throws IOException;
}