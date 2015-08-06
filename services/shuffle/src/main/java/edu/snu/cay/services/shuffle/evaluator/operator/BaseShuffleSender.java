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

import edu.snu.cay.services.shuffle.evaluator.operator.impl.BaseShuffleSenderImpl;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.transport.LinkListener;

import java.util.List;

/**
 * Shuffle sender that has base operation to send tuples.
 *
 * Users can register link listener for ShuffleTupleMessage to track whether the message
 * sent from this operator has been transferred successfully.
 */
@DefaultImplementation(BaseShuffleSenderImpl.class)
public interface BaseShuffleSender<K, V> extends ShuffleSender<K, V> {

  /**
   * Send a tuple to selected receivers using ShuffleStrategy of the shuffle.
   *
   * @param tuple a tuple
   * @return the selected receiver id list
   */
  List<String> sendTuple(Tuple<K, V> tuple);

  /**
   * Send a tupleList to selected receivers using ShuffleStrategy of the shuffle.
   *
   * Each tuple in the tupleList can be sent to many receivers, so the tuples to the same end point are
   * chunked into one ShuffleTupleMessage.
   *
   * @param tupleList a tuple list
   * @return the selected receiver id list
   */
  List<String> sendTuple(List<Tuple<K, V>> tupleList);

  /**
   * Send a tuple to the specific receiver.
   *
   * @param receiverId a receiver id
   * @param tuple a tuple
   */
  void sendTupleTo(String receiverId, Tuple<K, V> tuple);

  /**
   * Send a tuple list to the specific receiver. Note that this method does not use ShuffleStrategy to select
   * receivers and send all of tuples in tuple list to the same receiver.
   *
   * @param receiverId a receiver id
   * @param tupleList a tuple list
   */
  void sendTupleTo(String receiverId, List<Tuple<K, V>> tupleList);

  /**
   * Register a link listener to listen to whether the messages successfully sent through this sender.
   *
   * @param linkListener link listener for ShuffleTupleMessage
   */
  void registerTupleLinkListener(LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener);

}
