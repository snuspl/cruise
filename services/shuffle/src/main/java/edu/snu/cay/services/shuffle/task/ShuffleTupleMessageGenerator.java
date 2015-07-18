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
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 *
 */
@DefaultImplementation(ShuffleTupleMessageGeneratorImpl.class)
public interface ShuffleTupleMessageGenerator<K, V> {

  ShuffleTupleMessage<K, V> createTupleMessage(Tuple<K, V> tuple);

  ShuffleTupleMessage<K, V> createTupleMessage(List<Tuple<K, V>> tupleList);

  List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(Tuple<K, V> tuple);

  List<Tuple<String, ShuffleTupleMessage<K, V>>> createClassifiedTupleMessageList(List<Tuple<K, V>> tupleList);
}
