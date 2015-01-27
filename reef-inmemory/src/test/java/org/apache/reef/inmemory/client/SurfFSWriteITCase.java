package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.util.SurfLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test writing to Surf
 */
public final class SurfFSWriteITCase {

  private static final Logger LOG = Logger.getLogger(SurfFSWriteITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String SHORT = TESTDIR+"/"+"WRITE.short";
  private static final int SIZE1 = 1;

  private static final String LONG = TESTDIR+"/"+"WRITE.long";
  private static final int SIZE2 = 200;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final short REPLICATION = 3;
  private static final int BLOCK_SIZE = 1024; // Need to be a multiple of 512 (Hadoop Checksum Policy)
  private static final int BUFFER_SIZE = 4096;

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    final FSDataOutputStream stream1 = surfFs.create(new Path(SHORT), true, BUFFER_SIZE, REPLICATION, BLOCK_SIZE);
    for (int i = 0; i < SIZE1; i++) {
      stream1.write(b);
    }
    stream1.close();

    final FSDataOutputStream stream2 = surfFs.create(new Path(LONG), true, BUFFER_SIZE, REPLICATION, BLOCK_SIZE);
    for (int i = 0; i < SIZE2; i++) {
      stream2.write(b);
    }
    stream2.close();
  }

  @AfterClass
  public static void tearDownClass() {
    // surfFs.delete(new Path(TESTDIR), true); TODO: Enable when delete is implemented
    surfLauncher.close();
  }

  private void read(final String path, final int size) throws IOException {
    final FSDataInputStream in = surfFs.open(new Path(path));

    byte[] readBuf = new byte[size * b.length];

    int bytesRead = in.read(0, readBuf, 0, readBuf.length);
    assertEquals(bytesRead, readBuf.length);
    for (int i = 0; i < size * b.length; i++) {
      assertEquals("At index "+i, b[i % b.length], readBuf[i]);
    }
  }

  @Test
  public void testRead() throws IOException {
    read(SHORT, SIZE1);
    read(LONG, SIZE2);
  }

  @Test
  public void testExists() throws IOException {
    assertTrue(surfFs.exists(new Path(SHORT)));
    assertTrue(surfFs.exists(new Path(LONG)));
    assertFalse(surfFs.exists(new Path("Not_exists")));
  }
}