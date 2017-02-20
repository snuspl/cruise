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
package edu.snu.cay.common.datastorer;

import com.google.common.io.Files;
import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.hadoop.fs.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the DataStorer service.
 */
public final class DataStorerTest {
  private static final Tang TANG = Tang.Factory.getTang();

  private final String baseDirStr = Files.createTempDir().getAbsolutePath();
  private DataStorer dataStorer;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = TANG.newConfigurationBuilder()
        .bindNamedParameter(BaseDir.class, baseDirStr)
        .build();
    dataStorer = TANG.newInjector(conf).getInstance(LocalFSDataStorer.class);
  }

  /**
   * Checks whether the file is written successfully by DataStorer.
   * @throws IOException if a failure occurred while writing data.
   */
  @Test
  public void testLocalFSStorer() throws IOException {
    final String filename = "foo";
    final String strToWrite = "abc123";
    final byte[] bytesToWrite = strToWrite.getBytes();
    dataStorer.storeData(filename, bytesToWrite);

    final FileSystem fs = LocalFileSystem.get(new org.apache.hadoop.conf.Configuration());
    final Path writtenPath = new Path(baseDirStr, filename);
    try (FSDataInputStream dis = fs.open(writtenPath)) {
      final byte[] bytesToRead = new byte[bytesToWrite.length];
      final int numBytesRead = dis.read(bytesToRead);
      assertEquals("The numbers of written bytes are different", bytesToWrite.length, numBytesRead);
      assertTrue("The contents are different", Arrays.equals(bytesToWrite, bytesToRead));
    }
  }
}
