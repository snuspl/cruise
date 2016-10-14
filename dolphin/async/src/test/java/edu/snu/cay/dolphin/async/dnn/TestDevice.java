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
package edu.snu.cay.dolphin.async.dnn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Helper class to get test device options.
 */
public final class TestDevice {
  public static final String CPU = "CPU";
  public static final String GPU = "GPU";

  /**
   * Should not be instantiated.
   */
  private TestDevice() {
  }

  public static String[] getTestDevices() throws IOException {
    // read device option to run the test
    final InputStream is = TestDevice.class.getClassLoader().getResourceAsStream("dolphin-async.properties");
    final Properties p = new Properties();
    p.load(is);
    final String canRunGPU = p.getProperty("gpu");

    if (canRunGPU.equals("true")) {
      return new String[]{CPU, GPU};
    } else {
      return new String[]{CPU};
    }
  }
}
