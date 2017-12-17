/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.pregel.combiner;

/**
 * A interface for combiner that combines messages sent to the same vertex.
 * It combines two messages into one.
 *
 * @param <Long> vertex id
 * @param <M> message
 */
public interface MessageCombiner<Long, M> {

  /**
   * Combine messageToCombine with originalMessage.
   *
   * @param vertexId id of the vertex getting these messages
   * @param originalMessage the original message for the vertex
   * @param messageToCombine the new message to combine with the original message the second message
   */
  M combine(Long vertexId, M originalMessage, M messageToCombine);
}
