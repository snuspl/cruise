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

/**
 * Shuffle Service on REEF.
 *
 * Shuffle is a data movement abstraction where senders (mappers) transfer key/value
 * tuples to receivers (reducers) that are selected by a specific shuffling strategy.
 *
 * Shuffles can be registered to edu.snu.cay.services.shuffle.ShuffleDriver in the driver with
 * a ShuffleDescription that represents a basic structure of shuffles containing sender ids, receiver
 * ids, key/value codec and shuffling strategy class.
 *
 * A serialized shuffle description for a certain identifier is serialized and transferred from driver
 * to evaluator using Tang injector
 *
 * The edu.snu.cay.services.shuffle.ShuffleManager in the driver controls shuffle components in the evaluators
 * such as edu.snu.cay.services.shuffle.ShuffleOperator and edu.snu.cay.services.shuffle.Shuffle.
 * Users can get ShuffleOperator in evaluator-side code and send/receive tuples through the operators.
 *
 * Currently the Shuffle Service only supports push-based shuffle implementation.
 * We will add more types of shuffle implementations atop of the push primitive.
 */
package edu.snu.cay.services.shuffle;
