/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.ps.metric;

/**
 * Constants for PS metrics, which consists of client name of CentComm Service and
 * Keys to identify metrics.
 */
public final class ServerConstants {

  /**
   * Should not be instantiated.
   */
  private ServerConstants() {
  }

  public static final String CENT_COMM_CLIENT_NAME =
      "METRIC_COLLECTION_SERVICE_FOR_SERVER";
}
