/**
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
package edu.snu.cay.services.shuffle.task;

import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.List;

/**
 * Sending tuple operator. Users can register link listener for ShuffleTupleMessage to
 * track the sent message from this operator are executed successfully or make an exception.
 */
@DefaultImplementation(BaseTupleSender.class)
public interface TupleSender<K, V> extends TupleOperator<K, V> {

  /**
   * Send a tuple to selected receivers using ShuffleStrategy in the shuffle of the sender
   *
   * @param tuple a tuple
   * @return the selected receiver id list
   */
  List<String> sendTuple(Tuple<K, V> tuple);

  /**
   * Send a tuple list to selected receivers using ShuffleStrategy in the shuffle of the sender.
   * The tuples to same task are chunked into one network message.
   *
   * @param tupleList a tuple list
   * @return the selected receiver id list
   */
  List<String> sendTuple(List<Tuple<K, V>> tupleList);

  /**
   * Send a tuple to specific task
   *
   * @param destTaskId destination task id
   * @param tuple a tuple
   */
  void sendTupleTo(String destTaskId, Tuple<K, V> tuple);

  /**
   * Send a tuple list to specific task. Note that this method does not use ShuffleStrategy to select
   * receivers and send all of tuples in tuple list to the destination task.
   *
   * @param destTaskId destination task id
   * @param tupleList a tuple list
   */
  void sendTupleTo(String destTaskId, List<Tuple<K, V>> tupleList);

  /**
   * Register a link listener to listen the sent messages through this sender are successfully sent
   * or make an exception.
   *
   * @param linkListener link listener for ShuffleTupleMessage
   */
  void registerTupleLinkListener(LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener);

}
