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
package edu.snu.cay.services.shuffle.example.push;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters for MessageExchange example.
 */
public final class MessageExchangeParameters {

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Number of tasks that work as sender.
   */
  @NamedParameter(short_name = "sender_num", default_value = "8")
  public static final class SenderNumber implements Name<Integer> {
  }

  /**
   * Number of tasks that work as receiver.
   */
  @NamedParameter(short_name = "receiver_num", default_value = "8")
  public static final class ReceiverNumber implements Name<Integer> {
  }

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  @NamedParameter(short_name = "timeout", default_value = "60000")
  public static final class Timeout implements Name<Long> {
  }

  /**
   * Whether shutdown the application before all iterations are completed, or not.
   */
  @NamedParameter(short_name = "shutdown", default_value = "false")
  public static final class Shutdown implements Name<Boolean> {
  }

  /**
   * The number of total iterations.
   */
  @NamedParameter(short_name = "total_itr_num", default_value = "10")
  public static final class TotalIterationNum implements Name<Integer> {
  }

  /**
   * The threshold to shut down the application for the case where Shutdown is set to true.
   */
  @NamedParameter(short_name = "shutdown_itr_num", default_value = "3")
  public static final class ShutdownIterationNum implements Name<Integer> {
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private MessageExchangeParameters() {
  }
}
