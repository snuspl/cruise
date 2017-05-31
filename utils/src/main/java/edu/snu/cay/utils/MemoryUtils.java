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
package edu.snu.cay.utils;

/**
 * Utility class for managing memory.
 */
public final class MemoryUtils {
  private static final int MEGA_BYTES = 1048576;
  
  /**
   * Should not be instantiated.
   */
  private MemoryUtils() {
  }

  /**
   * @return The amount of memory used in the JVM heap. Although this value may be inaccurate, but is still useful
   * to track how the memory is being used approximately.
   */
  public static long getUsedMemoryMB() {
    final Runtime runtime = Runtime.getRuntime();
    return (runtime.totalMemory() - runtime.freeMemory()) / MEGA_BYTES;
  }
}
