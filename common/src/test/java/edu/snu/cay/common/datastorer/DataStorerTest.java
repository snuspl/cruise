package edu.snu.cay.common.datastorer;

import com.google.common.io.Files;
import edu.snu.cay.common.datastorer.param.BaseDir;
import org.apache.hadoop.fs.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the DataStorer service.
 */
public final class DataStorerTest {
  private static Tang TANG = Tang.Factory.getTang();

  private final String baseDirStr = Files.createTempDir().getAbsolutePath();
  private DataStorer dataStorer;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = TANG.newConfigurationBuilder()
        .bindNamedParameter(BaseDir.class, baseDirStr)
        .build();
    dataStorer = TANG.newInjector(conf).getInstance(LocalFSDataStorer.class);
  }

  @Test
  public void storeData() throws Exception {
    final String filename = "foo";
    final String strToWrite = "abc123";
    final byte[] bytesToWrite = strToWrite.getBytes();
    dataStorer.storeData(filename, bytesToWrite);

    final FileSystem fs = LocalFileSystem.get(new org.apache.hadoop.conf.Configuration());
    final Path writtenPath = new Path(baseDirStr, filename);
    try (final FSDataInputStream dis = fs.open(writtenPath)) {
      final byte[] bytesToRead = new byte[bytesToWrite.length];
      final int numBytesRead = dis.read(bytesToRead);
      assertEquals("The numbers of written bytes are different", bytesToWrite.length, numBytesRead);
      assertTrue("The contents are different", Arrays.equals(bytesToWrite, bytesToRead));
    }
  }
}