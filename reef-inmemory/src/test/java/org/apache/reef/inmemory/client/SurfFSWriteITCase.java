package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.SurfLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Test writing to Surf
 */
public final class SurfFSWriteITCase {

  private static final Logger LOG = Logger.getLogger(SurfFSWriteITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final byte[] b = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String SMALL = TESTDIR+"/"+"WRITE.short";
  private static final int SMALL_SIZE = 1;

  private static final String ONE_MB = TESTDIR+"/"+"WRITE.onemb";
  private static final int ONE_MB_SIZE = 1024 * 1024 / b.length;

  private static final String PACKET = TESTDIR+"/"+"WRITE.packet";
  private static final int PACKET_SIZE = SurfFSOutputStream.getPacketSize() / b.length;

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

    // FILE_SIZE < BLOCK_SIZE < PACKET_SIZE
    final FSDataOutputStream stream1 = surfFs.create(new Path(SMALL), true, BUFFER_SIZE, REPLICATION, BLOCK_SIZE);
    for (int i = 0; i < SMALL_SIZE; i++) {
      stream1.write(b);
    }
    stream1.close();

    // PACKET_SIZE > FILE_SIZE == BLOCK_SIZE
    final FSDataOutputStream stream2 = surfFs.create(new Path(ONE_MB), true, BUFFER_SIZE, REPLICATION, BLOCK_SIZE);
    for (int i = 0; i < ONE_MB_SIZE; i++) {
      stream2.write(b);
    }
    stream2.close();

    // FILE_SIZE == PACKET_SIZE < BLOCK_SIZE
    final FSDataOutputStream stream3 = surfFs.create(new Path(PACKET));
    for (int i = 0; i < PACKET_SIZE; i++) {
      stream3.write(b);
    }
    stream3.close();
  }

  @AfterClass
  public static void tearDownClass() {
    try {
      baseFs.delete(new Path(TESTDIR), true); // TODO: Delete when SurfFs.delete is implemented
      // surfFs.delete(new Path(TESTDIR), true); TODO: Enable when SurfFs.delete is implemented
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to delete " + TESTDIR, e);
    }
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
    read(SMALL, SMALL_SIZE);
    read(ONE_MB, ONE_MB_SIZE);
    read(PACKET, PACKET_SIZE);
  }

  @Test
  public void testExists() throws IOException {
    assertTrue("Should exist", surfFs.exists(new Path(SMALL)));
    assertTrue("Should exist", surfFs.exists(new Path(ONE_MB)));
    assertTrue("Should exist", surfFs.exists(new Path(PACKET)));
    assertFalse("Should not exist", surfFs.exists(new Path("Not_exists")));
  }
}