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
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Push-based shuffle sender.
 */
@DefaultImplementation(PushShuffleSenderImpl.class)
public interface PushShuffleSender<K, V> extends ShuffleSender<K, V> {

  void complete();

  void waitForReceivers();

  void completeAndWaitForReceivers();
}
