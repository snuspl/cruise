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

import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * MessageGenerator to make ShuffleTupleMessages from list of tuples.
 * It selects receivers using ShuffleStrategy of the shuffle and merge tuples to one ShuffleTupleMessage.
 */
@DefaultImplementation(ShuffleTupleMessageGeneratorImpl.class)
public interface ShuffleTupleMessageGenerator<K, V> {

  /**
   * Create a shuffle tuple message with a tuple
   *
   * @param tuple a tuple
   * @return a ShuffleTupleMessage
   */
  ShuffleTupleMessage<K, V> createTupleMessage(Tuple<K, V> tuple);

  /**
   * Create a shuffle tuple message with a tuple list
   *
   * @param tupleList a tuple list
   * @return a ShuffleTupleMessage
   */
  ShuffleTupleMessage<K, V> createTupleMessage(List<Tuple<K, V>> tupleList);

  /**
   * Create list of (String, ShuffleTupleMessage) pairs with a tuple. The key of the
   * returned tuple is the receiver evaluator id and the value is a merged ShuffleTupleMessage
   * including all tuples to the same evaluator. The tuples are classified by the ShuffleStrategy
   * of the shuffle.
   *
   * @param tuple a tuple
   * @return the list of (evaluator identifier, ShuffleTupleMessage) tuples
   */
  List<Tuple<String, ShuffleTupleMessage<K, V>>> createTupleMessageAndReceiverList(Tuple<K, V> tuple);

  /**
   * Create list of (String, ShuffleTupleMessage) pairs with a tuple list. The key of the
   * returned tuple is the receiver evaluator id and the value is a merged ShuffleTupleMessage
   * including all tuples to the same evaluator. The tuples are classified by the ShuffleStrategy
   * of the shuffle.
   *
   * @param tupleList a tuple list
   * @return the list of (evaluator identifier, ShuffleTupleMessage) tuples
   */
  List<Tuple<String, ShuffleTupleMessage<K, V>>> createTupleMessageAndReceiverList(List<Tuple<K, V>> tupleList);

}
