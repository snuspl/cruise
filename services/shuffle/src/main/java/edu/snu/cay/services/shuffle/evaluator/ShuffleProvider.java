/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cay.services.shuffle.evaluator;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Evaluator-side interface for providing Shuffles.
 *
 * Shuffles in the provider are automatically injected if the evaluator's configuration
 * was merged with proper configuration from ShuffleDriver.
 */
@DefaultImplementation(ShuffleProviderImpl.class)
public interface ShuffleProvider {

  /**
   * @param shuffleName name of the shuffle
   * @return the Shuffle instance named shuffleName
   */
  Shuffle getShuffle(String shuffleName);
}
