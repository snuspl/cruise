/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.shuffle.evaluator.operator;

import edu.snu.cay.services.shuffle.evaluator.operator.impl.PushShuffleSenderImpl;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Push-based shuffle sender.
 */
@DefaultImplementation(PushShuffleSenderImpl.class)
public interface PushShuffleSender<K, V> extends ShuffleSender<K, V> {

  /**
   * Send a tuple to selected receivers using ShuffleStrategy of the shuffle.
   *
   * Throws IllegalStateException if sender is waiting for receivers.
   *
   * @param tuple a tuple
   * @return the selected receiver id list
   */
  List<String> sendTuple(final Tuple<K, V> tuple);

  /**
   * Send a tupleList to selected receivers using ShuffleStrategy of the shuffle.
   *
   * Each tuple in the tupleList can be sent to many receivers, so the tuples to the same end point are
   * chunked into one ShuffleTupleMessage.
   *
   * Throws IllegalStateException if sender is waiting for receivers.
   *
   * @param tupleList a tuple list
   * @return the selected receiver id list
   */
  List<String> sendTuple(final List<Tuple<K, V>> tupleList);

  /**
   * Send a tuple to the specific receiver.
   *
   * Throws IllegalStateException if sender is waiting for receivers.
   *
   * @param receiverId a receiver id
   * @param tuple a tuple
   */
  void sendTupleTo(final String receiverId, final Tuple<K, V> tuple);

  /**
   * Send a tuple list to the specific receiver. Note that this method does not use ShuffleStrategy to select
   * receivers and send all of tuples in tuple list to the same receiver.
   *
   * Throws IllegalStateException if sender is waiting for receivers.
   *
   * @param receiverId a receiver id
   * @param tupleList a tuple list
   */
  void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList);

  /**
   * Complete one iteration of pushing data. The caller is blocking until a SENDER_CAN_SEND message
   * arrives from the manager.
   */
  void complete();

  /**
   * Completing the final iteration of pushing data, it makes receivers and manager be FINISHED.
   */
  void finish();

}
